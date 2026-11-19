import tempfile

import ray

from chia.base.ChiaFunction import chia_actor, get
from chia.base import dispatch_proxy as dp


def test_repeated_dispatches_reuse_function_exports():
    # Never connect this test to the production cluster.
    with tempfile.TemporaryDirectory(prefix='relay-exports-') as runtime:
        ray.init(address='local', num_cpus=2, include_dashboard=False,
                 object_store_memory=80*1024**2, _memory=512*1024**2,
                 _node_ip_address='127.0.0.1', _temp_dir=runtime,
                 log_to_driver=False)
        dp._proxy_handle = None
        try:
            method = chia_actor(dp.get_dispatch_proxy()).submit
            gcs = ray._private.worker.global_worker.gcs_client

            def exports():
                return set(gcs.internal_kv_keys(b'RemoteFunction:', b'fun'))

            def dispatch(value, profiled):
                profile = (f'call-{value}', {'display_name': 'export-test'}) if profiled else None
                # Vary options, positional arguments and keyword arguments.
                return method.chia_remote(
                    lambda x, offset: x + offset,
                    {'num_cpus': 0.5 if value % 2 else 1, 'max_retries': 0},
                    None, None, profile, (value,), {'offset': 7})

            # Each trampoline is registered on its first dispatch only.
            before = exports()
            assert get(dispatch(0, False), timeout=30) == 7
            plain = exports()
            assert len(plain - before) == 1
            assert get(dispatch(0, True), timeout=30) == 7
            warmed = exports()
            assert len(warmed - plain) == 1
            for _ in range(3):
                refs = [dispatch(i, profiled) for i in range(20)
                        for profiled in (False, True)]
                assert [get(ref, timeout=30) for ref in refs] == [
                    i + 7 for i in range(20) for _ in (False, True)]
                assert exports() == warmed
        finally:
            try:
                dp.release_dispatch_proxy()
            finally:
                ray.shutdown()
                dp._proxy_handle = None
