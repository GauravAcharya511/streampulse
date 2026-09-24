"""v0.5 -- checkpointing: snapshot/restore, atomic store, resumable runner."""
import pytest
from streampulse import (
    Event, ReplayableSource, Pipeline, StreamRunner, CheckpointStore,
    EventTimeTumblingWindow, KeyedAggregate, MemoryStateBackend,
)


def _stream(n=30, size=10):
    # deterministic keyed events across several windows
    return [Event(key="abc"[i % 3], value=i, event_time=i) for i in range(n)]


def test_checkpoint_store_roundtrip():
    store = CheckpointStore(MemoryStateBackend())
    assert store.load() is None
    store.save(7, [{"backend": {"k": "aGk="}}])
    got = store.load()
    assert got["offset"] == 7
    assert got["operators"][0]["backend"]["k"] == "aGk="


def test_window_snapshot_restore_roundtrip():
    b = MemoryStateBackend()
    op = EventTimeTumblingWindow(10, b)
    for e in _stream(15):
        list(op.push(e))
    snap = op.snapshot()

    b2 = MemoryStateBackend()
    op2 = EventTimeTumblingWindow(10, b2)
    op2.restore(snap)
    # restored operator continues identically to the original
    more = [Event("a", 100, 40)]
    assert list(op.push(more[0])) == list(op2.push(more[0]))


def test_keyed_aggregate_snapshot_restore():
    b = MemoryStateBackend()
    agg = KeyedAggregate(b)
    for e in _stream(9):
        list(agg.push(e))
    snap = agg.snapshot()
    agg2 = KeyedAggregate(MemoryStateBackend())
    agg2.restore(snap)
    e = Event("a", 5, 99)
    assert list(agg.push(e)) == list(agg2.push(e))


def _final_map(out):
    # last emitted result per window_start, for comparison
    m = {}
    for o in out:
        m[o.value["window_start"]] = o.value
    return m


def test_uninterrupted_vs_checkpointed_resume_identical():
    events = _stream(30, size=10)

    # (A) uninterrupted reference run
    ref = Pipeline(EventTimeTumblingWindow(10, MemoryStateBackend())).run(
        ReplayableSource(events))
    ref_map = _final_map(ref)

    # (B) run with checkpoints, then simulate a CRASH partway and RESUME
    ckpt_backend = MemoryStateBackend()          # stands in for a durable store
    store = CheckpointStore(ckpt_backend)

    # first runner processes the stream but we only let it checkpoint; then we
    # throw it away (crash) and build a fresh runner that restores from the store.
    src = ReplayableSource(events)
    op1 = EventTimeTumblingWindow(10, MemoryStateBackend())
    r1 = StreamRunner(Pipeline(op1), store=store, checkpoint_every=5)
    # emulate crash: process only the first 18 events, checkpointing every 5
    partial = ReplayableSource(events[:18])
    out1 = r1.run(partial, final_checkpoint=False)   # last checkpoint at offset 14

    # fresh operator + runner: restore from the checkpoint and finish the stream
    op2 = EventTimeTumblingWindow(10, MemoryStateBackend())
    r2 = StreamRunner(Pipeline(op2), store=store, checkpoint_every=5)
    out2 = r2.run(ReplayableSource(events), final_checkpoint=False)

    assert r2.restored_from == 14                 # resumed after last checkpoint
    resumed_map = _final_map(out2)
    # windows fully contained after the resume point must match the reference
    for ws in (10, 20):
        assert resumed_map[ws] == ref_map[ws]


def test_runner_with_lsmdb_checkpoint_store(tmp_path):
    pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend
    events = _stream(20, size=10)
    store = CheckpointStore(LsmdbStateBackend(str(tmp_path / "ckpt")))

    op = EventTimeTumblingWindow(10, MemoryStateBackend())
    r = StreamRunner(Pipeline(op), store=store, checkpoint_every=4)
    r.run(ReplayableSource(events[:12]), final_checkpoint=False)

    # a brand-new process restores from the durable lsmdb checkpoint and resumes
    op2 = EventTimeTumblingWindow(10, MemoryStateBackend())
    r2 = StreamRunner(Pipeline(op2), store=store, checkpoint_every=4)
    out = r2.run(ReplayableSource(events), final_checkpoint=False)
    assert r2.restored_from is not None
    assert any(o.value["window_start"] == 10 for o in out)
    store.close()
