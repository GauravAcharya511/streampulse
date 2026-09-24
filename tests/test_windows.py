"""v0.3 -- event-time tumbling windows + watermarks."""
import pytest
from streampulse import (
    Event, ReplayableSource, Pipeline, EventTimeTumblingWindow, MemoryStateBackend,
)


def _run(events, size=10, skew=0, backend=None):
    backend = backend or MemoryStateBackend()
    op = EventTimeTumblingWindow(size, backend, max_out_of_orderness=skew)
    out = Pipeline(op).run(ReplayableSource(events))
    return out, op


def test_tumbling_windows_aggregate_per_window():
    # window size 10: times 0..9 -> window [0,10); 10..19 -> [10,20)
    events = [Event("a", 1, 0), Event("a", 1, 5), Event("a", 1, 12), Event("a", 1, 25)]
    out, _ = _run(events, size=10)
    # window [0,10) fires when an event at t>=10 arrives (watermark reaches 10)
    fired = {(o.value["window_start"], o.value["window_end"]): o.value for o in out}
    assert fired[(0, 10)]["count"] == 2 and fired[(0, 10)]["sum"] == 2
    assert fired[(10, 20)]["count"] == 1
    assert fired[(20, 30)]["count"] == 1     # flushed at end of stream


def test_windows_fire_in_event_time_order():
    events = [Event("a", 1, t) for t in (0, 10, 20)]
    out, _ = _run(events, size=10)
    ends = [o.value["window_end"] for o in out]
    assert ends == sorted(ends)               # ascending window order


def test_window_fires_only_after_watermark_passes_end():
    # with no event past t=9, window [0,10) only fires at end-of-stream flush
    events = [Event("a", 1, 0), Event("a", 1, 9)]
    out, op = _run(events, size=10)
    assert len(out) == 1
    assert out[0].value == {"count": 2, "sum": 2, "window_start": 0, "window_end": 10}


def test_out_of_orderness_tolerance_keeps_late_arrival():
    # event at t=8 ARRIVES after t=12, but with skew=5 the watermark at t=12 is
    # 12-5=7 < 10, so window [0,10) hasn't fired yet and t=8 still counts.
    events = [Event("a", 1, 0), Event("a", 1, 12), Event("a", 1, 8)]
    out, op = _run(events, size=10, skew=5)
    w0 = [o.value for o in out if o.value["window_start"] == 0][0]
    assert w0["count"] == 2                    # t=0 and t=8 both in [0,10)
    assert op.late_count == 0


def test_late_event_dropped_and_counted():
    # skew=0: after t=20 arrives, watermark=20, window [0,10) has fired.
    # a subsequent t=3 event is late -> dropped.
    events = [Event("a", 1, 0), Event("a", 1, 20), Event("a", 1, 3)]
    out, op = _run(events, size=10)
    assert op.late_count == 1
    w0 = [o.value for o in out if o.value["window_start"] == 0][0]
    assert w0["count"] == 1                     # the late t=3 was NOT added


def test_windows_are_keyed():
    events = [Event("a", 1, 0), Event("b", 1, 0), Event("a", 1, 1), Event("x", 1, 15)]
    out, _ = _run(events, size=10)
    w0 = {o.key: o.value for o in out if o.value["window_start"] == 0}
    assert w0["a"]["count"] == 2 and w0["b"]["count"] == 1


def test_windowed_state_evicted_after_fire():
    backend = MemoryStateBackend()
    events = [Event("a", 1, 0), Event("a", 1, 20)]   # fires [0,10)
    _run(events, size=10, backend=backend)
    # after firing + end-of-stream flush, no per-window state should remain
    assert backend.get("a|0") is None
    assert backend.get("a|20") is None


def test_windows_with_lsmdb_backend(tmp_path):
    pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend
    backend = LsmdbStateBackend(str(tmp_path / "wstate"))
    events = [Event("a", 2, 0), Event("a", 3, 5), Event("a", 1, 25)]
    out = Pipeline(EventTimeTumblingWindow(10, backend)).run(ReplayableSource(events))
    w0 = [o.value for o in out if o.value["window_start"] == 0][0]
    assert w0 == {"count": 2, "sum": 5, "window_start": 0, "window_end": 10}
    backend.close()
