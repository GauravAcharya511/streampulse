"""Chains operators and runs a source through them.

v0.1 processes events one offset at a time so that, from v0.5, a checkpoint can
be taken between offsets and the source resumed from the last committed one.
`run` returns the list of output events; `run_tracked` also returns the last
offset processed (the seam checkpointing will hook into).
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
        out: List[Event] = []
        last_offset = start_offset - 1
        for off, event in source.read_from(start_offset):
            out.extend(self._apply([event]))
            last_offset = off
        return out, last_offset
