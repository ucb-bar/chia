"""Tests for chia.trace.metrics.MetricsLogger."""

import math
import os
import random
import tempfile

from chia.trace.metrics import MetricsLogger, NullBackend, TensorBoardBackend


def test_null_backend():
    m = MetricsLogger(backend="none")
    m.log_scalar("x", 1.0, step=0)
    m.flush()
    m.close()
    # double close is safe
    m.close()


def test_from_config_none():
    m = MetricsLogger.from_config(None)
    assert isinstance(m._backend, NullBackend)
    m.close()


def test_from_config_empty():
    m = MetricsLogger.from_config({})
    assert isinstance(m._backend, NullBackend)
    m.close()


def test_from_config_ignores_extra_kwargs():
    m = MetricsLogger.from_config({"backend": "none", "log_dir": "/tmp/unused"})
    assert isinstance(m._backend, NullBackend)
    m.close()


def test_unknown_backend_raises():
    try:
        MetricsLogger(backend="doesnotexist")
        assert False, "should have raised ValueError"
    except ValueError as e:
        assert "doesnotexist" in str(e)


def test_tensorboard_backend():
    with tempfile.TemporaryDirectory() as tmpdir:
        m = MetricsLogger.from_config({"backend": "tensorboard", "log_dir": tmpdir})
        assert isinstance(m._backend, TensorBoardBackend)
        for i in range(10):
            m.log_scalar("loss", 1.0 / (i + 1), step=i)
        m.flush()
        m.close()
        # tensorboardX writes event files into log_dir
        files = os.listdir(tmpdir)
        assert any("events.out.tfevents" in f for f in files), f"No event files in {files}"


def test_from_config_does_not_mutate_input():
    cfg = {"backend": "none", "extra": "value"}
    original = dict(cfg)
    MetricsLogger.from_config(cfg)
    assert cfg == original


TB_DEMO_DIR = "/tmp/chia_tb_demo"


def test_tensorboard_demo():
    """Log random curves to TensorBoard for visual inspection."""
    m = MetricsLogger.from_config({"backend": "tensorboard", "log_dir": TB_DEMO_DIR})
    random.seed(42)
    noise = 0.0
    for i in range(100):
        noise += random.gauss(0, 0.05)
        m.log_scalar("train/loss", math.exp(-0.03 * i) + noise + 0.1 * random.random(), step=i)
        m.log_scalar("train/accuracy", min(1.0, 0.5 + 0.005 * i + 0.05 * random.random()), step=i)
        m.log_scalar("eval/loss", math.exp(-0.025 * i) + 0.15 * random.random(), step=i)
    m.close()
    print(f"\n  TensorBoard logs written to {TB_DEMO_DIR}")
    print(f"  View with: tensorboard --logdir {TB_DEMO_DIR}")
    print(f"  Then open: http://localhost:6006\n")


if __name__ == "__main__":
    test_null_backend()
    print("test_null_backend: PASS")
    test_from_config_none()
    print("test_from_config_none: PASS")
    test_from_config_empty()
    print("test_from_config_empty: PASS")
    test_from_config_ignores_extra_kwargs()
    print("test_from_config_ignores_extra_kwargs: PASS")
    test_unknown_backend_raises()
    print("test_unknown_backend_raises: PASS")
    test_tensorboard_backend()
    print("test_tensorboard_backend: PASS")
    test_from_config_does_not_mutate_input()
    print("test_from_config_does_not_mutate_input: PASS")
    test_tensorboard_demo()
    print("test_tensorboard_demo: PASS")
    print("\nAll tests passed!")
