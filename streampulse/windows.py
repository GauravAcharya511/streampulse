"""Event-time tumbling windows: watermarks, allowed lateness, side-output, and
checkpointable state (all state on the instance, snapshot/restore supported).
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
        self.side_output = []
        self.late_count = 0
        self._pending = {}          # window_start -> set(keys)
        self._fired = set()         # (key, window_start)
        self._max_et = None

    def _skey(self, key, ws):
        return f"{key}|{ws}"

    def _window_start(self, t):
        return (t // self.size) * self.size

    def _emit(self, key, ws, we):
        state = self.agg.loads(self.backend.get(self._skey(key, ws)))
        out = self.agg.output(state)
        out.update({"window_start": ws, "window_end": we})
        return Event(key=key, value=out, event_time=we)

    def push(self, e):
        outs = []
        ws = self._window_start(e.event_time)
        we = ws + self.size
        cleanup = we + self.allowed_lateness

        if self.watermark is not None and self.watermark >= cleanup:
            self.side_output.append(e)
            self.late_count += 1
            return outs

        skey = self._skey(e.key, ws)
        blob = self.backend.get(skey)
        state = self.agg.loads(blob) if blob is not None else self.agg.initial()
        state = self.agg.combine(state, e)
        self.backend.put(skey, self.agg.dumps(state))
        self._pending.setdefault(ws, set()).add(e.key)

        if (e.key, ws) in self._fired:
            outs.append(self._emit(e.key, ws, we))     # late firing

        if self._max_et is None or e.event_time > self._max_et:
            self._max_et = e.event_time
            self.watermark = self._max_et - self.max_out_of_orderness
            outs.extend(self._on_watermark(self.watermark))
        return outs

    def flush(self):
        self.watermark = float("inf")
        return list(self._on_watermark(self.watermark))

    def _on_watermark(self, wm):
        for ws in sorted(self._pending):
            we = ws + self.size
            if we <= wm:
                for key in sorted(self._pending[ws]):
                    if (key, ws) not in self._fired:
                        self._fired.add((key, ws))
                        yield self._emit(key, ws, we)
        for ws in sorted(list(self._pending)):
            if ws + self.size + self.allowed_lateness <= wm:
                for key in list(self._pending[ws]):
                    self.backend.delete(self._skey(key, ws))
                    self._fired.discard((key, ws))
                del self._pending[ws]

    # ---- checkpointing ----
    def snapshot(self):
        return {
            "watermark": None if self.watermark in (None, float("inf")) else self.watermark,
            "max_et": self._max_et,
            "late_count": self.late_count,
            "pending": {str(ws): sorted(keys) for ws, keys in self._pending.items()},
            "fired": [[k, ws] for (k, ws) in self._fired],
            "side_output": [[e.key, e.value, e.event_time] for e in self.side_output],
            "backend": self.backend.snapshot(),
        }

    def restore(self, state):
        self.watermark = state["watermark"]
        self._max_et = state["max_et"]
        self.late_count = state["late_count"]
        self._pending = {int(ws): set(keys) for ws, keys in state["pending"].items()}
        self._fired = {(k, ws) for k, ws in state["fired"]}
        self.side_output = [Event(k, v, t) for k, v, t in state["side_output"]]
        self.backend.restore(state["backend"])
