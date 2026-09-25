"""Provider retry behavior without credentials, real waits or cluster changes."""
import copy
import json
import pickle
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from chia.models import opencode as oc
from chia.models.tests.test_opencode_sessions import export


@pytest.fixture(autouse=True)
def no_profiler(monkeypatch):
    monkeypatch.setattr('chia.trace.profiler.get_profiler', lambda: SimpleNamespace(enabled=False))
    monkeypatch.setattr(oc.OpenCodeLLM, '_get_node_id', lambda self: 'test-worker')


def install_provider(monkeypatch, llm, errors):
    """Return structured provider errors through the real classifier."""
    errors = iter(errors)
    calls, sleeps = [], []
    def invoke(message, tools):
        calls.append(message)
        llm._last_export_error = next(errors)
        return oc.OpenCodeQueryResult(result='OK', returncode=0, stderr='', stream_result='', success=False)
    monkeypatch.setattr(llm, '_run_opencode', invoke)
    monkeypatch.setattr('time.sleep', sleeps.append)
    monkeypatch.setattr(oc.random, 'uniform', lambda a, b: 1.0)
    return calls, sleeps


def error(status, message='temporarily unavailable', headers=None):
    return {'name': 'APIError', 'data': {
        'statusCode': status, 'message': message,
        'responseHeaders': headers or {}, 'isRetryable': True,
    }}


@pytest.mark.parametrize('failure', [error(429), error(503, 'Model is overloaded')])
def test_retry_succeeds_and_uses_separate_capacity_budget(monkeypatch, failure):
    llm = oc.OpenCodeLLM(retries=1)
    calls, sleeps = install_provider(monkeypatch, llm, [failure, failure, None])
    assert llm.prompt('hello').success
    assert calls == ['hello'] * 3
    assert sleeps == [15, 30]


@pytest.mark.parametrize('failure,kind', [
    (error(429), oc.RateLimitError),
    (error(503, 'Model is at capacity'), oc.ModelCapacityError),
])
def test_eight_attempts_no_sleep_after_exhaustion(monkeypatch, failure, kind):
    llm = oc.OpenCodeLLM(retries=5)
    calls, sleeps = install_provider(monkeypatch, llm, [failure] * 8)
    with pytest.raises(kind):
        llm.prompt('hello')
    assert len(calls) == 8
    assert sleeps == [15, 30, 60, 120, 240, 300, 300]


def test_capacity_budget_does_not_reset_after_server_error(monkeypatch):
    llm = oc.OpenCodeLLM(retries=5, capacity_attempts=3)
    calls, sleeps = install_provider(monkeypatch, llm, [error(429), error(500), error(429), error(429)])
    with pytest.raises(oc.RateLimitError):
        llm.prompt('hello')
    assert len(calls) == 4
    assert sleeps == [15, 5, 30]


@pytest.mark.parametrize('status,message', [(429, 'slow down'), (503, 'overloaded')])
@pytest.mark.parametrize('header', ['900', format_datetime(datetime.now(timezone.utc) + timedelta(hours=1))])
def test_retry_after_never_shortened(monkeypatch, status, message, header):
    llm = oc.OpenCodeLLM(retries=1)
    _, sleeps = install_provider(monkeypatch, llm, [error(status, message, {'Retry-After': header}), None])
    assert llm.prompt('hello').success
    assert 899 <= sleeps[0] <= 3601


@pytest.mark.parametrize('header', ['bad-date', '-2', 'nan', 'inf', '1e300'])
def test_bad_retry_after_uses_backoff(monkeypatch, header):
    llm = oc.OpenCodeLLM(retries=1)
    _, sleeps = install_provider(monkeypatch, llm, [error(429, headers={'retry-after': header}), None])
    assert llm.prompt('hello').success
    assert sleeps == [15]


def test_jitter(monkeypatch):
    llm = oc.OpenCodeLLM(retries=1)
    _, sleeps = install_provider(monkeypatch, llm, [error(429), None])
    def jitter(low, high):
        assert (low, high) == (.8, 1.2)
        return 1.2
    monkeypatch.setattr(oc.random, 'uniform', jitter)
    assert llm.prompt('hello').success
    assert sleeps == [18]


@pytest.mark.parametrize('failure,kind', [
    (error(401), oc.AuthenticationError),
    (error(403), oc.AuthenticationError),
    (error(402), oc.BillingError),
    (error(429, 'insufficient_quota'), oc.BillingError),
    (error(429, 'Credit balance exhausted'), oc.BillingError),
    ({'name': 'APIError', 'data': {'statusCode': 400, 'message': 'bad request'}}, oc.InvalidRequestError),
])
def test_permanent_errors_fail_immediately(monkeypatch, failure, kind):
    llm = oc.OpenCodeLLM()
    calls, sleeps = install_provider(monkeypatch, llm, [failure])
    with pytest.raises(kind):
        llm.prompt('hello')
    assert len(calls) == 1 and not sleeps


def test_rate_quota_is_not_confused_with_exhausted_credits(monkeypatch):
    llm = oc.OpenCodeLLM(retries=1)
    _, sleeps = install_provider(monkeypatch, llm, [error(429, 'Requests per minute quota exceeded'), None])
    assert llm.prompt('hello').success
    assert sleeps == [15]


def test_server_retry_has_no_final_sleep(monkeypatch):
    llm = oc.OpenCodeLLM(retries=2)
    calls, sleeps = install_provider(monkeypatch, llm, [error(500)] * 2)
    assert not llm.prompt('hello').success
    assert len(calls) == 2 and sleeps == [5]


@pytest.mark.parametrize('failure', [error(429), error(503, 'overloaded')])
def test_retry_preserves_completed_tools_and_counts_usage_once(monkeypatch, tmp_path, failure):
    llm = oc.OpenCodeLLM(resume_session=True, retries=1, work_dir=str(tmp_path))
    baseline = export('old'); llm.restore_session(json.dumps(baseline).encode())
    failed = export('old', 'partial', 'failed')
    failed['messages'][1]['parts'].append({
        'type': 'tool', 'tool': 'probe', 'callID': 'call_1',
        'state': {'status': 'completed', 'input': {}, 'output': 'saved result'},
    })
    failed['messages'][2]['info'].update(error=failure, cost=0,
        tokens={'input': 0, 'output': 0, 'reasoning': 0, 'cache': {'read': 0, 'write': 0}})
    succeeded = copy.deepcopy(failed)
    succeeded['messages'] += export('new')['messages']
    states = iter([failed, succeeded]); imports = []; commands = []; sleeps = []
    def capture(cmd, env):
        commands.append(cmd[1])
        if cmd[1] == 'import':
            imports.append(json.loads(Path(cmd[2]).read_text()))
        if cmd[1] == 'run':
            assert cmd[-3:-1] == ['--session', 'ses_portable']
        payload = json.dumps(next(states)) if cmd[1] == 'export' else json.dumps({'sessionID': 'ses_portable'})
        return SimpleNamespace(returncode=0, stdout=payload, stderr='')
    monkeypatch.setattr(llm, '_capture', capture)
    monkeypatch.setattr('time.sleep', sleeps.append)
    result = llm.prompt('continue')
    assert result.success and result.result == 'new'
    assert imports == [baseline, failed]
    assert commands == ['import', 'run', 'export'] * 2
    assert result.usage['cost_usd'] == pytest.approx(.04)
    assert result.usage['input_tokens'] == 6
    assert result.usage['output_tokens'] == 8
    assert result.usage['reasoning_tokens'] == 4
    assert result.usage['cache_read'] == 10
    assert json.loads(result.session_transcript) == succeeded
    assert [m['info']['id'] for m in result.call_export['messages']] == ['partial', 'failed', 'new']
    assert len(sleeps) == 1


def test_capacity_error_roundtrips_between_workers():
    exc = oc.ModelCapacityError('node', 1, 'overloaded', 120)
    restored = pickle.loads(pickle.dumps(exc))
    assert (restored.node_id, restored.error_type, restored.retry_after) == ('node', 'model_capacity', 120)


@pytest.mark.parametrize('attempts', [0, -1, 1.5, True])
def test_invalid_attempt_limit(attempts):
    with pytest.raises(ValueError):
        oc.OpenCodeLLM(capacity_attempts=attempts)
