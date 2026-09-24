"""Event-time tumbling windows with watermarks, allowed lateness, and a
side-output for late data (v0.4).

Lifecycle of a (key, window):
  1. Accumulating   -- events fold into state while watermark < window_end.
  2. Fire           -- when watermark >= window_end, emit the aggregate.
  3. Allowed-late   -- state is KEPT until watermark >= window_end + allowed_lateness.
                       A late event landing here folds in and RE-FIRES the window
                       with an updated result (Flink's "late firing").
  4. Cleanup/evict  -- once watermark >= window_end + allowed_lateness, state is
                       dropped. Any later event for that window is too late and is
                       routed to `side_output` instead of corrupting a result.

`max_out_of_orderness` sets how far the watermark trails the max event time;
`allowed_lateness` is the extra grace after firing before a window is closed.
Per-(key,window) state lives in any StateBackend (durable on lsmdb).
"""
from .event import Event
from .operators import Operator
from .aggregate import RunningCountSum


class EventTimeTumblingWindow(Operator):
    def __init__(self, size, backend, aggregator=None,
                 max_out_of_orderness=0, allowed_lateness=0):
        assert size > 0
        self.size = size
        self.backend = backend
        self.agg = aggregator or RunningCountSum()
        self.max_out_of_orderness = max_out_of_orderness
        self.allowed_lateness = allowed_lateness
        self.watermark = None
        self.side_output = []          # events too late even for allowed_lateness
        self.late_count = 0            # == len(side_output); kept for convenience
        self._pending = {}             # window_start -> set(keys) with live state
        self._fired = set()            # (key, window_start) already emitted at least once

    def _skey(self, key, ws):
        return f"{key}|{ws}"

    def _window_start(self, t):
        return (t // self.size) * self.size

    def _emit(self, key, ws, we):
        state = self.agg.loads(self.backend.get(self._skey(key, ws)))
        out = self.agg.output(state)
        out.update({"window_start": ws, "window_end": we})
        return Event(key=key, value=out, event_time=we)

    def process(self, events):
        max_et = None
        for e in events:
            ws = self._window_start(e.event_time)
            we = ws + self.size
            cleanup = we + self.allowed_lateness

            # too late: window already closed -> side-output, don't touch state
            if self.watermark is not None and self.watermark >= cleanup:
                self.side_output.append(e)
                self.late_count += 1
                continue

            skey = self._skey(e.key, ws)
            blob = self.backend.get(skey)
            state = self.agg.loads(blob) if blob is not None else self.agg.initial()
            state = self.agg.combine(state, e)
            self.backend.put(skey, self.agg.dumps(state))
            self._pending.setdefault(ws, set()).add(e.key)

            # late-but-allowed: window already fired -> re-fire with updated result
            if (e.key, ws) in self._fired:
                yield self._emit(e.key, ws, we)

            if max_et is None or e.event_time > max_et:
                max_et = e.event_time
                self.watermark = max_et - self.max_out_of_orderness
                yield from self._on_watermark(self.watermark)

        self.watermark = float("inf")
        yield from self._on_watermark(self.watermark)

    def _on_watermark(self, wm):
        # first fire: windows whose end the watermark has passed
        for ws in sorted(self._pending):
            we = ws + self.size
            if we <= wm:
                for key in sorted(self._pending[ws]):
                    if (key, ws) not in self._fired:
                        self._fired.add((key, ws))
                        yield self._emit(key, ws, we)
        # cleanup: windows past their allowed-lateness horizon
        for ws in sorted(list(self._pending)):
            if ws + self.size + self.allowed_lateness <= wm:
                for key in list(self._pending[ws]):
                    self.backend.delete(self._skey(key, ws))
                    self._fired.discard((key, ws))
                del self._pending[ws]
