"""Persistent JSON state store with atomic writes and an advisory lock."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile


class StateStore:
    """Whole-state JSON document persisted under ``state_dir/state.json``.

    Every mutation is followed by :meth:`save`, which writes a temp file and
    atomically renames it over the real one, so a crash (even kill -9 or a
    power cut) can never leave a half-written state file behind.
    """

    def __init__(self, state_dir: str):
        self.state_dir = os.path.abspath(state_dir)
        os.makedirs(self.state_dir, exist_ok=True)
        self.path = os.path.join(self.state_dir, "state.json")
        self.data: dict = {"version": 1, "tasks": {}, "idempotency_keys": {}}

    def load(self) -> "StateStore":
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                self.data = json.load(fh)
        self.data.setdefault("version", 1)
        self.data.setdefault("tasks", {})
        self.data.setdefault("idempotency_keys", {})
        return self

    def save(self) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.state_dir, prefix=".state-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self.data, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def tasks(self) -> dict:
        return self.data["tasks"]


class StateLock:
    """Advisory flock so only one mutating process uses a state_dir at a time."""

    def __init__(self, state_dir: str):
        self.path = os.path.join(state_dir, "taskflow.lock")
        self._fd: int | None = None

    def __enter__(self) -> "StateLock":
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise RuntimeError(
                f"another taskflow process is active for this state_dir ({self.path})"
            ) from exc
        return self

    def __exit__(self, *exc_info) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
