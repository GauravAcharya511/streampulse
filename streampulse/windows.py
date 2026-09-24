"""Event-time tumbling windows with watermarks -- the core of stream processing.

A TUMBLING window of size W partitions event-time into fixed, non-overlapping
buckets: an event at time t belongs to window [floor(t/W)*W, +W). Aggregation is
per (key, window).

A WATERMARK is the engine's estimate of event-time progress: "no event earlier
than this is still expected." We use bounded out-of-orderness --
    watermark = max_event_time_seen - max_out_of_orderness
-- so a window only fires once we're confident all its events have arrived. A
window [ws, we) fires (emits its aggregate and evicts its state) when the
watermark reaches we. Events for an already-fired window are LATE (counted here;
handled in v0.4).

Per-(key,window) state lives in any StateBackend, so windowed aggregation is
durable when backed by lsmdb.
"""
from .event import Event
from .operators import Operator
from .aggregate import RunningCountSum


class EventTimeTumblingWindow(Operator):
    def __init__(self, size, backend, aggregator=None, max_out_of_orderness=0):
        assert size > 0
        self.size = size
        self.backend = backend
        self.agg = aggregator or RunningCountSum()
        self.max_out_of_orderness = max_out_of_orderness
        self.watermark = None            # None = -infinity (nothing seen yet)
        self.late_count = 0
        self._pending = {}               # window_start -> set(keys) with live state

    def _skey(self, key, ws):
        return f"{key}|{ws}"

    def _window_start(self, t):
        return (t // self.size) * self.size

    def process(self, events):
        max_et = None
        for e in events:
            ws = self._window_start(e.event_time)
            we = ws + self.size
            # late: the window already fired (its end is at/under the watermark)
            if self.watermark is not None and we <= self.watermark:
                self.late_count += 1
                continue
            skey = self._skey(e.key, ws)
            blob = self.backend.get(skey)
            state = self.agg.loads(blob) if blob is not None else self.agg.initial()
            state = self.agg.combine(state, e)
            self.backend.put(skey, self.agg.dumps(state))
            self._pending.setdefault(ws, set()).add(e.key)

            if max_et is None or e.event_time > max_et:
                max_et = e.event_time
                self.watermark = max_et - self.max_out_of_orderness
                yield from self._fire_ready(self.watermark)

        # end of stream: advance watermark to +inf and flush all windows
        self.watermark = float("inf")
        yield from self._fire_ready(self.watermark)

    def _fire_ready(self, wm):
        ready = [ws for ws in self._pending if ws + self.size <= wm]
        for ws in sorted(ready):
            we = ws + self.size
            for key in sorted(self._pending[ws]):
                skey = self._skey(key, ws)
                state = self.agg.loads(self.backend.get(skey))
                out = self.agg.output(state)
                out.update({"window_start": ws, "window_end": we})
                self.backend.delete(skey)         # evict fired state
                yield Event(key=key, value=out, event_time=we)
            del self._pending[ws]
