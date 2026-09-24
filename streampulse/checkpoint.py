"""Durable checkpoint store.

A checkpoint captures, atomically, the source OFFSET processed so far plus every
operator's state snapshot. It is written as a single JSON blob under one key, so
one backend `put` commits the whole checkpoint atomically (in the lsmdb backend
that put is WAL-durable). On restart, `load` returns the last committed
checkpoint; a crash mid-write leaves the *previous* checkpoint intact.
"""
import json

_KEY = "__checkpoint__"


class CheckpointStore:
    def __init__(self, backend):
        self._backend = backend        # a durable StateBackend (e.g. lsmdb)

    def save(self, offset: int, operator_snaps: list) -> None:
        blob = json.dumps({"offset": offset, "operators": operator_snaps}).encode("utf-8")
        self._backend.put(_KEY, blob)   # single atomic commit

    def load(self):
        blob = self._backend.get(_KEY)
        return json.loads(blob.decode("utf-8")) if blob is not None else None

    def close(self):
        self._backend.close()
