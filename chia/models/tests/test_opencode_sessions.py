"""Portable OpenCode sessions; no provider credentials or cluster required."""
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from chia.base.ChiaFunction import ObjectRefCallback, get
from chia.models import opencode as oc


def export(*ids):
    return {'info': {'id': 'ses_portable'}, 'messages': [
        {'info': {'id': identity, 'role': 'assistant', 'cost': .02,
                  'tokens': {'input': 3, 'output': 4, 'reasoning': 2,
                             'cache': {'read': 5, 'write': 0}}},
         'parts': [{'type': 'text', 'text': identity}]} for identity in ids]}


@pytest.fixture(autouse=True)
def disable_profiler(monkeypatch):
    monkeypatch.setattr('chia.trace.profiler.get_profiler', lambda: SimpleNamespace(
        enabled=False, on_remote_complete=lambda result: result))


@pytest.mark.parametrize("fresh_worker", [False, True])
def test_resume_isolates_db_and_counts_only_new_messages(monkeypatch, tmp_path, fresh_worker):
    original_data = tmp_path/'original'; auth = original_data/'opencode'/'auth.json'
    auth.parent.mkdir(parents=True); auth.write_text('{"test":"fake-token"}')
    monkeypatch.setenv('XDG_DATA_HOME', str(original_data))
    calls = []; homes = []; current = export('m1'); imported = []
    def capture(self, cmd, env):
        calls.append(cmd[1]); home = env['XDG_DATA_HOME']; homes.append(home)
        assert home != str(original_data)
        assert (Path(home)/'opencode'/'auth.json').read_text() == auth.read_text()
        if cmd[1] == 'import':
            imported.append(json.loads(Path(cmd[2]).read_text()))
        elif cmd[1] == 'run':
            if imported:
                assert cmd[-3:-1] == ['--session', 'ses_portable']
            return SimpleNamespace(returncode=0, stdout=json.dumps({'sessionID':'ses_portable'}), stderr='')
        elif cmd[1] == 'export':
            return SimpleNamespace(returncode=0, stdout=json.dumps(current), stderr='')
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    monkeypatch.setattr(oc.OpenCodeLLM, '_capture', capture)
    worker1 = oc.OpenCodeLLM(resume_session=True, retries=1)
    first = worker1.prompt('first')
    assert first.success and first.usage['input_tokens'] == 3
    assert all(not Path(home).exists() for home in homes)
    worker2 = oc.OpenCodeLLM(resume_session=True, retries=1) if fresh_worker else worker1
    if fresh_worker:
        worker2.restore_session(first.session_transcript)
    current = export('m1', 'm2')
    second = worker2.prompt('second')
    assert second.success and second.result == 'm2'
    assert second.usage['cost_usd'] == .02
    assert second.usage['input_tokens'] == 3
    assert second.call_export == {'messages': [current['messages'][1]]}
    assert json.loads(second.session_transcript) == current
    assert imported == [export('m1')]
    assert calls == ['run','export','import','run','export']
    assert len(set(homes)) == 2
    assert all(not Path(home).exists() for home in homes)
    assert os.environ['XDG_DATA_HOME'] == str(original_data)
    assert auth.read_text() == '{"test":"fake-token"}'


@pytest.mark.parametrize('change', ['lost','duplicate','cost','tokens'])
def test_rejects_lost_or_recounted_history(change):
    old = export('m1'); new = export('m1','m2')
    if change == 'lost': new['messages'].pop(0)
    if change == 'duplicate': new['messages'].append(copy.deepcopy(new['messages'][0]))
    if change == 'cost': new['messages'][0]['info']['cost'] += 1
    if change == 'tokens': new['messages'][0]['info']['tokens']['output'] += 1
    with pytest.raises(ValueError): oc.OpenCodeLLM._current_call_export(old,new)


def test_import_failure_does_not_start_new_conversation(monkeypatch):
    llm = oc.OpenCodeLLM(resume_session=True, retries=1)
    previous = json.dumps(export('m1')).encode(); llm.restore_session(previous)
    commands=[]; homes=[]
    def capture(cmd,env):
        commands.append(cmd); homes.append(env['XDG_DATA_HOME'])
        return SimpleNamespace(returncode=1, stdout='', stderr='bad import')
    monkeypatch.setattr(llm,'_capture',capture)
    result=llm.prompt('next')
    assert not result.success
    assert json.loads(result.session_transcript) == json.loads(previous)
    assert [c[1] for c in commands] == ['import']
    assert all(not Path(home).exists() for home in homes)


@pytest.mark.parametrize('options', [False,True])
def test_remote_get_updates_driver_state(monkeypatch, options):
    llm=oc.OpenCodeLLM(resume_session=True)
    result=oc.OpenCodeQueryResult(result='ok',returncode=0,stderr='',stream_result='',
                                 session_transcript=json.dumps(export('m1')).encode())
    raw=object()
    monkeypatch.setattr(oc.OpenCodeLLM.prompt,'chia_remote',lambda *a,**kw:raw)
    monkeypatch.setattr(oc.OpenCodeLLM.prompt,'options',lambda **kw:SimpleNamespace(chia_remote=lambda *a,**k:raw))
    handle=llm.prompt.options(resources={'opencode_creds':1}) if options else llm.prompt
    ref=handle.chia_remote(llm,'hello')
    assert isinstance(ref,ObjectRefCallback)
    assert llm._session_export is None
    monkeypatch.setattr('ray.get',lambda *a,**kw:result)
    assert get(ref) is result
    assert llm._session_export == export('m1')


def test_retry_does_not_replay_old_error_and_counts_new_attempts(monkeypatch):
    llm=oc.OpenCodeLLM(resume_session=True,retries=2)
    first=export('m1'); first['messages'][0]['info']['error']={
        'name':'MessageAbortedError','data':{'message':'abort'}}
    second=copy.deepcopy(first); second['messages'] += export('m2')['messages']
    states=iter([first,second]); commands=[]
    def capture(cmd,env):
        commands.append(cmd[1])
        payload=json.dumps(next(states)) if cmd[1]=='export' else json.dumps({'sessionID':'ses_portable'})
        return SimpleNamespace(returncode=0,stdout=payload,stderr='')
    monkeypatch.setattr(llm,'_capture',capture)
    result=llm.prompt('continue')
    assert result.success and result.result=='m2'
    assert result.usage['cost_usd']==.04
    assert commands==['run','export','import','run','export']


def test_restore_rejects_invalid_session_and_requires_opt_in():
    with pytest.raises(ValueError): oc.OpenCodeLLM().restore_session(json.dumps(export('m1')).encode())
    llm=oc.OpenCodeLLM(resume_session=True)
    bad=export('m1'); bad['info']['id']='--help'
    with pytest.raises(ValueError): llm.restore_session(json.dumps(bad).encode())
    assert llm._session_export is None


def test_concurrent_agents_keep_session_state_separate(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    barrier = Barrier(2)
    homes = {}

    def capture(self, cmd, env):
        if cmd[1] == 'run':
            identity = cmd[-1]
            homes[identity] = env['XDG_DATA_HOME']
            barrier.wait(timeout=5)
            assert len(set(homes.values())) == 2
            return SimpleNamespace(returncode=0, stdout=json.dumps({'sessionID': 'ses_'+identity}), stderr='')
        assert cmd[1] == 'export'
        identity = cmd[-1].removeprefix('ses_')
        state = export(identity)
        state['info']['id'] = cmd[-1]
        return SimpleNamespace(returncode=0, stdout=json.dumps(state), stderr='')

    monkeypatch.setattr(oc.OpenCodeLLM, '_capture', capture)
    first = oc.OpenCodeLLM(resume_session=True, retries=1)
    second = oc.OpenCodeLLM(resume_session=True, retries=1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [pool.submit(first.prompt, 'alpha'), pool.submit(second.prompt, 'beta')]
        results = [task.result(timeout=10) for task in tasks]
    assert [result.result for result in results] == ['alpha', 'beta']
    assert first._session_export == json.loads(results[0].session_transcript)
    assert second._session_export == json.loads(results[1].session_transcript)
    assert first._session_export['info']['id'] != second._session_export['info']['id']
    assert all(not Path(home).exists() for home in homes.values())
