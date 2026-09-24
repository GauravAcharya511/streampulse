"""The unit that flows through the pipeline.

`key`        -- what state/windows are grouped by (v0.2+).
`value`      -- the payload.
`event_time` -- WHEN the event actually happened (ms since epoch), which may be
                out of order relative to arrival. Event-time (not processing-time)
                is what windows and watermarks reason about (v0.3+).
"""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Event:
    key: str
    value: Any
    event_time: int          # milliseconds since epoch

    def with_value(self, value: Any) -> "Event":
        return Event(self.key, value, self.event_time)
