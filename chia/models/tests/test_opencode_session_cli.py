"""Real CLI portability against a local HTTP fixture, never a paid provider.

Set OPENCODE_SESSION_TEST_BIN to a local OpenCode binary (tested with 1.18.32).
"""
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import ray.cloudpickle as pickle

from chia.models.opencode import AdditionalModelProvider, OpenCodeLLM


def test_real_cli_resumes_export_with_fresh_database(monkeypatch, tmp_path):
    binary = os.environ.get('OPENCODE_SESSION_TEST_BIN')
    if not binary:
        pytest.skip('set OPENCODE_SESSION_TEST_BIN for local mock-provider integration')
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(body)
            base = {'id': 'chatcmpl-test', 'object': 'chat.completion.chunk',
                    'created': 1700000000, 'model': 'model'}
            start = {**base, 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': 'OK'}, 'finish_reason': None}]}
            end = {**base, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}],
                   'usage': {'prompt_tokens': 100, 'completion_tokens': 10, 'total_tokens': 110,
                             'prompt_tokens_details': {'cached_tokens': 20},
                             'completion_tokens_details': {'reasoning_tokens': 2}}}
            if body.get('stream'):
                data = ('data: '+json.dumps(start)+'\n\ndata: '+json.dumps(end)+'\n\ndata: [DONE]\n\n').encode()
                content = 'text/event-stream'
            else:
                end['object'] = 'chat.completion'
                end['choices'] = [{'index': 0, 'message': {'role': 'assistant', 'content': 'OK'}, 'finish_reason': 'stop'}]
                data = json.dumps(end).encode(); content = 'application/json'
            self.send_response(200)
            self.send_header('Content-Type', content)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers(); self.wfile.write(data)

    monkeypatch.setenv('OPENCODE_CONFIG_CONTENT', json.dumps({
        'small_model': 'local/model', 'share': 'disabled', 'snapshot': False}))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path/'config'))
    monkeypatch.setenv('XDG_DATA_HOME', str(tmp_path/'worker-data'))
    monkeypatch.setenv('OPENCODE_DISABLE_AUTOUPDATE', '1')
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        state = None
        results = []
        for turn in range(2):
            llm = OpenCodeLLM(
                model='local/model', system_message='Say OK.', opencode_bin=binary,
                resume_session=True, retries=1, timeout_seconds=90,
                work_dir=str(tmp_path/'work'), dangerously_skip_permissions=False,
                additional_providers=[AdditionalModelProvider(
                    id='local', api_key='fake', base_url=f'http://127.0.0.1:{server.server_port}/v1',
                    models={'model': {'name': 'Test', 'limit': {'context': 32768, 'output': 8192},
                                      'cost': {'input': .22, 'cache_read': .007, 'output': .66}}})])
            llm.restore_session(state)
            # Separate Python workers share only the serialized Chia object.
            # No running Ray cluster or external provider is contacted.
            worker = subprocess.run(
                [sys.executable, "-c", """
import sys
from types import SimpleNamespace
import ray.cloudpickle as pickle
import chia.trace.profiler
chia.trace.profiler.get_profiler = lambda: SimpleNamespace(enabled=False)
llm, prompt = pickle.loads(sys.stdin.buffer.read())
sys.stdout.buffer.write(pickle.dumps(llm.prompt(prompt)))
"""],
                input=pickle.dumps((llm, 'Say OK.' if turn == 0 else 'Say OK again.')),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=110,
            )
            assert worker.returncode == 0, worker.stderr.decode()
            result = pickle.loads(worker.stdout)
            assert result.success, result.stderr
            state = result.session_transcript
            assert len(json.loads(state)['messages']) == 2 * (turn + 1)
            assert result.usage['input_tokens'] == 80
            assert result.usage['output_tokens'] == 8
            assert result.usage['reasoning_tokens'] == 2
            assert result.usage['cache_read'] == 20
            assert result.usage['cost_usd'] == pytest.approx(.00002434)
            results.append(result)
        assert results[0].session_id == results[1].session_id
        # The second model request actually contains the first exchange.
        assert any(sum(m['role'] == 'user' for m in req['messages']) == 2
                   and any(m['role'] == 'assistant' for m in req['messages']) for req in requests)
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=5)
