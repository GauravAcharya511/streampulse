"""Stateless operators for v0.1. Each transforms a stream of events into another
stream. Stateful operators (keyed aggregation, windows) arrive in v0.2+.
"""
from typing import Callable, Iterator

from .event import Event


class Operator:
    def process(self, events: Iterator[Event]) -> Iterator[Event]:
        raise NotImplementedError


class Map(Operator):
    """Apply fn(event) -> event to each event."""
    def __init__(self, fn: Callable[[Event], Event]):
        self._fn = fn

    def process(self, events):
        for e in events:
            yield self._fn(e)


class Filter(Operator):
    """Keep only events where predicate(event) is True."""
    def __init__(self, predicate: Callable[[Event], bool]):
        self._pred = predicate

    def process(self, events):
        for e in events:
            if self._pred(e):
                yield e
