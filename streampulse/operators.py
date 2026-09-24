"""Operators.

Each operator is a standing, stateful stage. The runtime drives it one event at a
time via `push`, and signals end-of-stream via `flush`. All operator state lives
on the instance (never trapped in a generator frame), so it can be snapshotted
for checkpointing (v0.5) via `snapshot`/`restore`.

`process` is a convenience that runs push over an iterable then flush -- the
Pipeline (v0.1-0.4) uses it, so all earlier behavior is preserved exactly.
"""
from typing import Callable, Iterable

from .event import Event


class Operator:
    def push(self, event: Event) -> Iterable[Event]:
        raise NotImplementedError

    def flush(self) -> Iterable[Event]:
        return ()

    def process(self, events):
        for e in events:
            yield from self.push(e)
        yield from self.flush()

    # checkpointing hooks -- stateless operators need nothing
    def snapshot(self) -> dict:
        return {}

    def restore(self, state: dict) -> None:
        return None


class Map(Operator):
    def __init__(self, fn: Callable[[Event], Event]):
        self._fn = fn

    def push(self, event):
        return (self._fn(event),)


class Filter(Operator):
    def __init__(self, predicate: Callable[[Event], bool]):
        self._pred = predicate

    def push(self, event):
        return (event,) if self._pred(event) else ()
