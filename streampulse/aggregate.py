"""Stateful keyed aggregation -- the first operator that carries state.

For each event, KeyedAggregate loads the running state for event.key from a
StateBackend, folds the event in, persists the new state, and emits an event
carrying the current aggregate. State is serialized to bytes so it can live in
any backend (a dict, or lsmdb on disk).

The Aggregator is pluggable; RunningCountSum tracks per-key count and sum and
emits {"count": c, "sum": s}.
"""
import json
from typing import Any

from .event import Event
from .operators import Operator
from .state import StateBackend


class Aggregator:
    def initial(self) -> Any: ...
    def combine(self, state: Any, event: Event) -> Any: ...
    def output(self, state: Any) -> Any: ...
    def dumps(self, state: Any) -> bytes:
        return json.dumps(state).encode("utf-8")
    def loads(self, blob: bytes) -> Any:
        return json.loads(blob.decode("utf-8"))


class RunningCountSum(Aggregator):
    def initial(self):
        return {"count": 0, "sum": 0}

    def combine(self, state, event):
        return {"count": state["count"] + 1, "sum": state["sum"] + event.value}

    def output(self, state):
        return dict(state)


class KeyedAggregate(Operator):
    def __init__(self, backend: StateBackend, aggregator: Aggregator = None):
        self._backend = backend
        self._agg = aggregator or RunningCountSum()

    def process(self, events):
        for e in events:
            blob = self._backend.get(e.key)
            state = self._agg.loads(blob) if blob is not None else self._agg.initial()
            state = self._agg.combine(state, e)
            self._backend.put(e.key, self._agg.dumps(state))
            yield e.with_value(self._agg.output(state))
