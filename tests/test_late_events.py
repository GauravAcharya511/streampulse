"""v0.4 -- allowed lateness (late firing) + side-output for too-late events."""
import pytest
from streampulse import (
    Event, ReplayableSource, Pipeline, EventTimeTumblingWindow, MemoryStateBackend,
)


def _run(events, size=10, skew=0, lateness=0, backend=None):
    backend = backend or MemoryStateBackend()
    op = EventTimeTumblingWindow(size, backend,
                                 max_out_of_orderness=skew, allowed_lateness=lateness)
    out = Pipeline(op).run(ReplayableSource(events))
    return out, op


def test_late_event_within_allowed_lateness_refires():
    # window [0,10) fires at t=10; a late t=4 arrives while watermark(15) < 10+10=20,
    # so it folds in and the window RE-FIRES with an updated count.
    events = [Event("a", 1, 0), Event("a", 1, 10), Event("a", 1, 15), Event("a", 1, 4)]
    out, op = _run(events, size=10, lateness=10)
    w0 = [o.value for o in out if o.value["window_start"] == 0]
    assert w0[0]["count"] == 1          # first fire (only t=0 was in [0,10))
    assert w0[-1]["count"] == 2         # late firing after t=4 folds in
    assert op.late_count == 0           # t=4 was allowed, not dropped


def test_event_too_late_goes_to_side_output():
    # allowed_lateness=5: window [0,10) closes at watermark >= 15. A t=3 event
    # arriving once watermark=20 is too late -> side-output, state untouched.
    events = [Event("a", 1, 0), Event("a", 1, 20), Event("a", 1, 3)]
    out, op = _run(events, size=10, lateness=5)
    assert len(op.side_output) == 1
    assert op.side_output[0].event_time == 3
    w0 = [o.value for o in out if o.value["window_start"] == 0]
    assert w0[-1]["count"] == 1         # the too-late event was NOT counted


def test_allowed_lateness_boundary_closes_window():
    # window [0,10), lateness=5 -> cleanup at watermark >= 15.
    # event at t=15 makes watermark=15 -> window closes; a later t=2 is side-output.
    events = [Event("a", 1, 0), Event("a", 1, 15), Event("a", 1, 2)]
    out, op = _run(events, size=10, lateness=5)
    assert len(op.side_output) == 1 and op.side_output[0].event_time == 2


def test_no_side_output_when_everything_on_time():
    events = [Event("a", 1, t) for t in (0, 3, 11, 12, 25)]
    out, op = _run(events, size=10, lateness=5)
    assert op.side_output == []
    assert op.late_count == 0


def test_late_firing_persists_with_lsmdb(tmp_path):
    pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend
    backend = LsmdbStateBackend(str(tmp_path / "late"))
    events = [Event("a", 5, 0), Event("a", 5, 12), Event("a", 5, 3)]  # t=3 late-but-allowed
    op = EventTimeTumblingWindow(10, backend, allowed_lateness=10)
    out = Pipeline(op).run(ReplayableSource(events))
    w0 = [o.value for o in out if o.value["window_start"] == 0]
    assert w0[-1] == {"count": 2, "sum": 10, "window_start": 0, "window_end": 10}
    assert op.side_output == []
    backend.close()
