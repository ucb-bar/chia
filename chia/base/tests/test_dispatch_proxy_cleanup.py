import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pytest
import ray

from chia.base.ChiaFunction import ChiaFunction, _ChiaActorMethod, chia_actor, get
from chia.base import dispatch_proxy as dp
from chia.base.pid_registry import chia_cancel


@ChiaFunction(max_retries=0)
def sleeping_child(path):
    child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'], start_new_session=True)
    Path(path).write_text(str(child.pid))
    child.wait()


def wait_for(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.05)
    raise AssertionError('cleanup did not finish within 15 seconds')


def alive(path):
    if not path.exists():
        return False
    stat = Path(f'/proc/{path.read_text()}/stat')
    return stat.exists() and stat.read_text().split()[2] != 'Z'


def test_reaper_preserves_live_unknown_and_other_namespace(monkeypatch):
    monkeypatch.setattr(ray._private.state, 'jobs', lambda: [dict(JobID='dead', IsDead=True), dict(JobID='live', IsDead=False)])
    monkeypatch.setattr(ray.util, 'list_named_actors', lambda **_: [
        dict(namespace=ns, name='chia_dispatch_proxy_'+job)
        for ns, job in [('chia_dispatch','dead'),('chia_dispatch','live'),('chia_dispatch','unknown'),('other','dead')]])
    released = []
    monkeypatch.setattr(dp, 'release_dispatch_proxy', lambda job: released.append(job) or True)
    assert dp.reap_dead_dispatch_proxies() == ['dead']
    assert released == ['dead']


def test_legacy_relay_requires_separate_drain_check(monkeypatch):
    monkeypatch.setattr(ray, 'get_actor', lambda *a, **k: object())
    killed = []
    monkeypatch.setattr(ray, 'kill', lambda *a, **k: killed.append(True))
    with pytest.raises(AttributeError):
        dp.release_dispatch_proxy('legacy')
    assert not killed


def test_cancel_release_and_reap_crashed_driver_without_affecting_live_job(tmp_path):
    # Explicit isolated cluster: never connect this destructive test to production.
    runtime = tempfile.TemporaryDirectory(prefix='relay-cleanup-')
    context = ray.init(address='local', num_cpus=2, include_dashboard=False,
                       object_store_memory=80*1024**2, _memory=512*1024**2,
                       _node_ip_address='127.0.0.1', _temp_dir=runtime.name, log_to_driver=False)
    dp._proxy_handle = None
    try:
        proxy = dp.get_dispatch_proxy()
        method = chia_actor(proxy).submit
        result = method.chia_remote(lambda: 42, {'max_retries':0}, None, None, None, (), {})
        assert get(result, timeout=10) == 42
        assert get(_ChiaActorMethod(proxy.__ray_call__).chia_remote(lambda actor: len(actor._active)), timeout=10) == 0
        failed = method.chia_remote(lambda: 1/0, {'max_retries':0}, None, None, None, (), {})
        with pytest.raises(ray.exceptions.RayTaskError):
            get(failed, timeout=10)
        assert get(_ChiaActorMethod(proxy.__ray_call__).chia_remote(lambda actor: len(actor._active)), timeout=10) == 0
        canceled_path, live_path = tmp_path/'canceled.pid', tmp_path/'live.pid'
        def dispatch(path):
            return method.chia_remote(sleeping_child._chia_original, {'max_retries':0}, None, None, None, (str(path),), {})
        canceled, live = dispatch(canceled_path), dispatch(live_path)
        wait_for(lambda: canceled_path.exists() and live_path.exists())
        chia_cancel(canceled, force=False)
        wait_for(lambda: not alive(canceled_path))
        assert alive(live_path)

        # A separate driver dies without finally cleanup, leaving a detached relay.
        dead_path, identity = tmp_path/'dead.pid', tmp_path/'dead.json'
        script = f'''import json,os,time,ray
from pathlib import Path
from chia.base.ChiaFunction import ChiaFunction,chia_actor
from chia.base.dispatch_proxy import get_dispatch_proxy
import subprocess,sys
@ChiaFunction(max_retries=0)
def child(path):
 p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)
 Path(path).write_text(str(p.pid));p.wait()
ray.init(address={context.address_info['gcs_address']!r},log_to_driver=False)
p=get_dispatch_proxy()
r=chia_actor(p).submit.chia_remote(child._chia_original,{{'max_retries':0}},None,None,None,({str(dead_path)!r},),{{}})
while not Path({str(dead_path)!r}).exists():time.sleep(.05)
Path({str(identity)!r}).write_text(json.dumps({{'job_id':str(ray.get_runtime_context().get_job_id())}}))
os._exit(0)
'''
        # Give the dead driver's child a CPU; the live control uses no CPU reservation.
        chia_cancel(live, force=False)
        wait_for(lambda: not alive(live_path))
        live = method.chia_remote(sleeping_child._chia_original, {'max_retries':0,'num_cpus':0}, None, None, None, (str(live_path),), {})
        wait_for(lambda: alive(live_path))
        subprocess.run([sys.executable, '-c', script], check=True, timeout=40)
        job_id = json.loads(identity.read_text())['job_id']
        wait_for(lambda: any(str(j['JobID']) == job_id and j['IsDead'] for j in ray._private.state.jobs()))
        assert job_id in dp.reap_dead_dispatch_proxies()
        wait_for(lambda: not alive(dead_path))
        assert alive(live_path)
        with pytest.raises(ValueError):
            ray.get_actor('chia_dispatch_proxy_'+job_id, namespace='chia_dispatch')
        assert dp.release_dispatch_proxy()
        wait_for(lambda: not alive(live_path))
        assert not dp.release_dispatch_proxy()  # Idempotent normal/exception cleanup.
    finally:
        try:
            dp.release_dispatch_proxy()
        finally:
            ray.shutdown()
            dp._proxy_handle = None
            runtime.cleanup()
