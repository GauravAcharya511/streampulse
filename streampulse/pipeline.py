"""Chains operators and runs a source through them.

The operator chain is applied ONCE over the whole event stream -- operators are
standing, stateful stages that see a continuous stream (a windowing or
aggregation operator keeps state across events; it must not be re-instantiated
per event). Offsets are tracked as a side effect of pulling from the source: the
last offset seen is available for checkpointing (v0.5).
"""
from typing import List, Tuple

from .event import Event
from .source import ReplayableSource
from .operators import Operator


class Pipeline:
    def __init__(self, *operators: Operator):
        self._operators = list(operators)

    def _apply(self, events):
        stream = iter(events)
        for op in self._operators:
            stream = op.process(stream)
        return stream

    def run(self, source: ReplayableSource, start_offset: int = 0) -> List[Event]:
        out, _ = self.run_tracked(source, start_offset)
        return out

    def run_tracked(self, source: ReplayableSource, start_offset: int = 0
                    ) -> Tuple[List[Event], int]:
        tracker = {"last": start_offset - 1}

        def tracked_events():
            for off, event in source.read_from(start_offset):
                tracker["last"] = off
                yield event

        out = list(self._apply(tracked_events()))
        return out, tracker["last"]
