"""A replayable, offset-addressed event source.

Every event has a monotonic integer OFFSET. A source can be replayed from any
offset, which is the foundation for checkpoint/restore (v0.5): a checkpoint
records the offset processed so far, and on restart we resume from there.
"""
import random
from typing import Iterator, Tuple

from .event import Event


class ReplayableSource:
    def __init__(self, events):
        self._events = list(events)

    def __len__(self) -> int:
        return len(self._events)

    def read_from(self, offset: int = 0) -> Iterator[Tuple[int, Event]]:
        """Yield (offset, event) pairs starting at `offset` (0-based)."""
        for off in range(offset, len(self._events)):
            yield off, self._events[off]


def gen_events(n: int, keys=("a", "b", "c"), max_skew_ms: int = 0, seed: int = 0):
    """Generate n events with event_time increasing by ~1ms each, optionally
    perturbed by up to max_skew_ms to simulate out-of-order arrival (used from
    v0.3 onward). Returns a list suitable for ReplayableSource."""
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        base = i  # ms
        skew = rnd.randint(-max_skew_ms, max_skew_ms) if max_skew_ms else 0
        et = max(0, base + skew)
        out.append(Event(key=rnd.choice(keys), value=1, event_time=et))
    return out
