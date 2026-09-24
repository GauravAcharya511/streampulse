"""Pluggable state backends for keyed state (the Flink pattern).

A backend is a durable-or-not key/value store the streaming operators use to hold
per-key aggregation state. Two implementations:

  MemoryStateBackend -- a dict; fast, non-durable, used by most tests.
  LsmdbStateBackend  -- persists state in lsmdb, the LSM-tree storage engine this
                        project is built on. State survives process restarts,
                        which is what makes checkpoint/restore (v0.5) meaningful.
"""
from abc import ABC, abstractmethod


class StateBackend(ABC):
    @abstractmethod
    def get(self, key: str):
        """Return stored bytes for key, or None."""

    @abstractmethod
    def put(self, key: str, value: bytes) -> None:
        ...

    def close(self) -> None:
        pass


class MemoryStateBackend(StateBackend):
    def __init__(self):
        self._d = {}

    def get(self, key: str):
        return self._d.get(key)

    def put(self, key: str, value: bytes) -> None:
        self._d[key] = value


class LsmdbStateBackend(StateBackend):
    """Keyed state persisted in lsmdb. Requires the `lsmdb` package
    (pip install git+https://github.com/GauravAcharya511/lsmdb.git)."""

    def __init__(self, path: str, **kwargs):
        from lsmdb import LSMDB          # imported lazily so the dep is optional
        self._db = LSMDB(path, **kwargs)

    def get(self, key: str):
        return self._db.get(key)         # bytes or None

    def put(self, key: str, value: bytes) -> None:
        self._db.put(key, value)

    def close(self) -> None:
        self._db.close()
