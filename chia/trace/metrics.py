"""Generic metrics logger with pluggable backends (TensorBoard, W&B).

Usage:
    from chia.trace import MetricsLogger

    metrics = MetricsLogger(backend="tensorboard", log_dir="./runs/my_experiment")
    metrics.log_scalar("loss", 0.5, step=0)
    metrics.close()

Backends are optional dependencies — the core chia package does not require
tensorboardX or wandb.  A clear ImportError is raised at construction time
if the requested backend's dependency is missing.

NOTE: MetricsLogger is NOT serializable and must NOT be passed to
@ChiaFunction / ray.remote.  It should live on the head-node orchestrator.
"""

from __future__ import annotations

import atexit
import threading
from abc import ABC, abstractmethod


class MetricsBackend(ABC):
    """Abstract base for metrics backends."""

    @abstractmethod
    def log_scalar(self, tag: str, value: float, step: int) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class NullBackend(MetricsBackend):
    """No-op backend for when metrics logging is disabled."""

    def __init__(self, **kwargs):
        pass  # accept and ignore all kwargs

    def log_scalar(self, tag: str, value: float, step: int) -> None:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class TensorBoardBackend(MetricsBackend):
    """Backend using tensorboardX (lightweight, no PyTorch dependency)."""

    def __init__(self, log_dir: str = "./runs", **kwargs):
        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            raise ImportError(
                "tensorboardX is required for the TensorBoard backend. "
                "Install it with: pip install tensorboardX"
            )
        self._writer = SummaryWriter(logdir=log_dir, **kwargs)

    def log_scalar(self, tag: str, value: float, step: int) -> None:
        self._writer.add_scalar(tag, value, step)

    def flush(self) -> None:
        self._writer.flush()

    def close(self) -> None:
        self._writer.close()


class WandbBackend(MetricsBackend):
    """Backend using Weights & Biases."""

    def __init__(self, project: str = "chia", run_name: str | None = None, **kwargs):
        try:
            import wandb
        except ImportError:
            raise ImportError(
                "wandb is required for the W&B backend. "
                "Install it with: pip install wandb"
            )
        self._wandb = wandb
        wandb.init(project=project, name=run_name, **kwargs)

    def log_scalar(self, tag: str, value: float, step: int) -> None:
        self._wandb.log({tag: value}, step=step)

    def flush(self) -> None:
        pass  # wandb auto-flushes

    def close(self) -> None:
        self._wandb.finish()


_BACKENDS: dict[str, type[MetricsBackend]] = {
    "tensorboard": TensorBoardBackend,
    "wandb": WandbBackend,
    "none": NullBackend,
}


class MetricsLogger:
    """Backend-agnostic metrics logger.

    Args:
        backend: One of "tensorboard", "wandb", or "none".
        **kwargs: Forwarded to the backend constructor.
            TensorBoard: log_dir (str), flush_secs (int), ...
            W&B: project (str), run_name (str), entity (str), config (dict), ...
    """

    def __init__(self, backend: str = "tensorboard", **kwargs):
        if backend not in _BACKENDS:
            raise ValueError(
                f"Unknown metrics backend {backend!r}. "
                f"Available: {', '.join(_BACKENDS)}"
            )
        self._backend = _BACKENDS[backend](**kwargs)
        self._lock = threading.Lock()
        self._closed = False
        atexit.register(self.close)

    @classmethod
    def from_config(cls, cfg: dict | None) -> MetricsLogger:
        """Create a MetricsLogger from a config dict.

        Expected format: {"backend": "tensorboard", "log_dir": "./runs", ...}
        All keys besides "backend" are forwarded to the backend constructor.
        Returns a no-op logger when cfg is None or empty.
        """
        if not cfg:
            return cls(backend="none")
        cfg = dict(cfg)  # shallow copy to avoid mutating caller's dict
        backend = cfg.pop("backend", "none")
        return cls(backend=backend, **cfg)

    def log_scalar(self, tag: str, value: float, step: int) -> None:
        with self._lock:
            if not self._closed:
                self._backend.log_scalar(tag, value, step)

    def flush(self) -> None:
        with self._lock:
            if not self._closed:
                self._backend.flush()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._backend.close()
