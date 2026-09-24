"""Reproducible benchmark for streampulse.

Measures:
  1. Windowed event-time aggregation throughput (events/sec).
  2. Checkpointing overhead: throughput with durable lsmdb checkpoints vs without.
  3. Recovery time: how long to restore operator state + offset from a checkpoint.

Run:  python benchmark.py [N]     (default N = 200_000)
Requires the lsmdb backend:  pip install -e ".[lsmdb]"
"""
import os
import sys
import time
import shutil

from streampulse import (
    Event, ReplayableSource, Pipeline, StreamRunner, CheckpointStore,
    EventTimeTumblingWindow, KeyedAggregate, MemoryStateBackend, LsmdbStateBackend,
)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000


def _events(n):
    keys = ["a", "b", "c", "d", "e"]
    return [Event(keys[i % 5], (i % 7) + 1, i) for i in range(n)]


def bench_window_throughput(events):
    op = EventTimeTumblingWindow(1000, MemoryStateBackend())
    t = time.time()
    Pipeline(op).run(ReplayableSource(events))
    dt = time.time() - t
    return len(events) / dt, dt


def bench_checkpoint_overhead(events, workdir):
    # baseline: no checkpointing
    b = MemoryStateBackend()
    t = time.time()
    StreamRunner(Pipeline(KeyedAggregate(b))).run(ReplayableSource(events))
    base = len(events) / (time.time() - t)

    # with durable lsmdb checkpoints every 1000 events
    shutil.rmtree(workdir, ignore_errors=True)
    store = CheckpointStore(LsmdbStateBackend(workdir))
    b2 = MemoryStateBackend()
    t = time.time()
    StreamRunner(Pipeline(KeyedAggregate(b2)), store=store, checkpoint_every=1000)\
        .run(ReplayableSource(events), final_checkpoint=True)
    ckpt = len(events) / (time.time() - t)
    store.close()
    shutil.rmtree(workdir, ignore_errors=True)
    return base, ckpt


def bench_recovery_time(events, workdir):
    shutil.rmtree(workdir, ignore_errors=True)
    store = CheckpointStore(LsmdbStateBackend(workdir))
    b = MemoryStateBackend()
    StreamRunner(Pipeline(KeyedAggregate(b)), store=store, checkpoint_every=1000)\
        .run(ReplayableSource(events), final_checkpoint=True)
    store.close()

    # time a cold restore: open store, restore operator state, ready to resume
    t = time.time()
    store2 = CheckpointStore(LsmdbStateBackend(workdir))
    b2 = MemoryStateBackend()
    r = StreamRunner(Pipeline(KeyedAggregate(b2)), store=store2)
    ckpt = store2.load()
    for op, snap in zip(r._ops(), ckpt["operators"]):
        op.restore(snap)
    dt = (time.time() - t) * 1000
    store2.close()
    shutil.rmtree(workdir, ignore_errors=True)
    return dt, ckpt["offset"]


def main():
    events = _events(N)
    wd = "bench_ckpt"

    wtp, wdt = bench_window_throughput(events)
    base, ckpt = bench_checkpoint_overhead(events, wd)
    rec_ms, off = bench_recovery_time(events, wd)

    print("=" * 60)
    print(f"streampulse benchmark  (N = {N:,} events)")
    print("=" * 60)
    print(f"Windowed aggregation:   {wtp:,.0f} events/sec  ({wdt:.2f}s)")
    print()
    print("Checkpointing overhead  (keyed aggregation)")
    print(f"  no checkpoints:       {base:,.0f} events/sec")
    print(f"  durable ckpt/1000:    {ckpt:,.0f} events/sec  "
          f"({(1-ckpt/base)*100:.1f}% overhead)")
    print()
    print(f"Recovery from checkpoint (state at offset {off:,}):  {rec_ms:.1f} ms")
    print("=" * 60)


if __name__ == "__main__":
    main()
