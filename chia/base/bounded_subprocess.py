"""Drain build logs without retaining unbounded compiler output."""
import os
import selectors
import signal
import subprocess
import time


class _Output:
    def __init__(self, limit):
        self.keep = (limit - 256) // 2
        self.head = bytearray()
        self.tail = bytearray()
        self.total = 0

    def append(self, data):
        self.total += len(data)
        take = min(len(data), self.keep - len(self.head))
        self.head.extend(data[:take])
        self.tail.extend(data[take:])
        del self.tail[:max(0, len(self.tail) - self.keep)]

    def text(self):
        omitted = self.total - len(self.head) - len(self.tail)
        marker = f'\n... compiler output truncated: {omitted} bytes omitted ...\n'.encode() if omitted else b''
        return (self.head + marker + self.tail).decode('utf-8', errors='replace')


def run_bounded(command, cwd, timeout, *, limit=1024 * 1024):
    """Return stdout, stderr, rc; retain at most ``limit`` raw bytes per stream.

    Keep the beginning and end, drain excess bytes, and kill the complete
    process group on timeout/cancellation. Binary outputs belong in files.
    """
    if limit < 256:
        raise ValueError('output limit must be at least 256 bytes')
    outputs = [_Output(limit), _Output(limit)]
    deadline = time.monotonic() + timeout
    proc = subprocess.Popen(command, cwd=cwd, shell=isinstance(command, str),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True)
    timed_out = False
    completed = False
    try:
        with selectors.DefaultSelector() as selector:
            for stream, output in zip((proc.stdout, proc.stderr), outputs):
                selector.register(stream, selectors.EVENT_READ, output)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                for key, _ in selector.select(remaining):
                    data = os.read(key.fd, 65536)
                    if data:
                        key.data.append(data)
                    else:
                        selector.unregister(key.fileobj)
            proc.wait(timeout=max(0, deadline - time.monotonic()))
            completed = True
    except subprocess.TimeoutExpired:
        timed_out = True
    finally:
        # Also kill descendants holding pipes after their parent exits.
        if not completed:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        proc.wait()
        proc.stdout.close()
        proc.stderr.close()
    stdout, stderr = (output.text() for output in outputs)
    if timed_out:
        stderr += f'\nExecution timed out (timeout after {timeout}s).'
    return stdout, stderr, -1 if timed_out else proc.returncode
