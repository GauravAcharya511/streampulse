"""v0.6 -- exactly-once semantics under a mid-stream crash (deterministic, no
timing). A checkpointed run is interrupted; a fresh runner restores from the last
checkpoint and finishes. Final per-key aggregates must equal a ground-truth run:
no event lost, none counted twice."""
import pytest
from streampulse import (
    Event, ReplayableSource, Pipeline, StreamRunner, CheckpointStore,
    KeyedAggregate, MemoryStateBackend,
)


def _dataset(n=40):
    keys = ["a", "b", "c", "d"]
    return [Event(keys[i % 4], (i % 5) + 1, i) for i in range(n)]


def _ground_truth(events):
    totals = {}
    for e in events:
        t = totals.setdefault(e.key, {"count": 0, "sum": 0})
        t["count"] += 1
        t["sum"] += e.value
    return totals


def _totals_from_backend(backend, keys):
    import json
    out = {}
    for k in keys:
        blob = backend.get(k)
        if blob is not None:
            out[k] = json.loads(blob.decode("utf-8"))
    return out


def test_exactly_once_after_midstream_crash():
    events = _dataset(40)
    truth = _ground_truth(events)
    keys = list(truth)

    durable = MemoryStateBackend()          # stands in for the durable ckpt store
    store = CheckpointStore(durable)

    # ---- process 1: runs partway, checkpointing every 5, then "crashes" ----
    b1 = MemoryStateBackend()
    r1 = StreamRunner(Pipeline(KeyedAggregate(b1)), store=store, checkpoint_every=5)
    r1.run(ReplayableSource(events[:23]), final_checkpoint=False)   # dies at offset 22
    # b1 (in-memory working state) is LOST with the crash; only `store` survives.

    # ---- process 2: fresh state, restores from checkpoint, finishes ----
    b2 = MemoryStateBackend()
    r2 = StreamRunner(Pipeline(KeyedAggregate(b2)), store=store, checkpoint_every=5)
    r2.run(ReplayableSource(events), final_checkpoint=True)

    assert r2.restored_from == 19            # last checkpoint before the crash (n=20)
    got = _totals_from_backend(b2, keys)
    assert got == truth                      # EXACTLY once: no loss, no double count


def test_multiple_crashes_still_exactly_once():
    events = _dataset(50)
    truth = _ground_truth(events)
    keys = list(truth)
    store = CheckpointStore(MemoryStateBackend())

    # three interrupted attempts, each resuming from the last checkpoint
    for stop in (12, 27, 41):
        b = MemoryStateBackend()
        r = StreamRunner(Pipeline(KeyedAggregate(b)), store=store, checkpoint_every=4)
        r.run(ReplayableSource(events[:stop]), final_checkpoint=False)
    # final clean run to completion
    bf = MemoryStateBackend()
    rf = StreamRunner(Pipeline(KeyedAggregate(bf)), store=store, checkpoint_every=4)
    rf.run(ReplayableSource(events), final_checkpoint=True)

    assert _totals_from_backend(bf, keys) == truth


def test_exactly_once_with_durable_lsmdb_store(tmp_path):
    pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend
    events = _dataset(30)
    truth = _ground_truth(events)
    keys = list(truth)
    store = CheckpointStore(LsmdbStateBackend(str(tmp_path / "ckpt")))

    b1 = MemoryStateBackend()
    StreamRunner(Pipeline(KeyedAggregate(b1)), store=store, checkpoint_every=5)\
        .run(ReplayableSource(events[:18]), final_checkpoint=False)

    b2 = MemoryStateBackend()
    StreamRunner(Pipeline(KeyedAggregate(b2)), store=store, checkpoint_every=5)\
        .run(ReplayableSource(events), final_checkpoint=True)
    assert _totals_from_backend(b2, keys) == truth
    store.close()
