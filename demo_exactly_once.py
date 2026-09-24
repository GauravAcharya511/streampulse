"""Exactly-once crash-recovery demo -- with a REAL process kill (SIGKILL).

Driver:
  1. computes ground-truth per-key totals for a fixed dataset;
  2. spawns a worker subprocess that streams the data slowly, checkpointing
     operator state + source offset to a durable lsmdb store every few events;
  3. SIGKILLs the worker mid-stream (a hard crash -- no cleanup, no flush);
  4. spawns a fresh worker that restores from the last checkpoint and finishes;
  5. asserts the final totals equal ground truth -- no loss, no double count.

Run:  python demo_exactly_once.py
Requires the lsmdb backend:  pip install "git+https://github.com/GauravAcharya511/lsmdb.git"
"""
import os
import sys
import json
import time
import signal
import shutil
import subprocess

from streampulse import (
    Event, ReplayableSource, Pipeline, StreamRunner, CheckpointStore,
    KeyedAggregate, MemoryStateBackend, LsmdbStateBackend,
)

WORK = "eo_demo"
CKPT = os.path.join(WORK, "ckpt")
RESULT = os.path.join(WORK, "result.json")
N = 120
DELAY = 0.02          # seconds per event, so we can kill mid-stream


def dataset(n=N):
    keys = ["a", "b", "c", "d"]
    return [Event(keys[i % 4], (i % 5) + 1, i) for i in range(n)]


def ground_truth(events):
    t = {}
    for e in events:
        d = t.setdefault(e.key, {"count": 0, "sum": 0})
        d["count"] += 1
        d["sum"] += e.value
    return t


class SlowSource:
    """A source that sleeps per event so the driver can kill the worker mid-run."""
    def __init__(self, events, delay):
        self._events = events
        self._delay = delay

    def read_from(self, offset=0):
        for off in range(offset, len(self._events)):
            time.sleep(self._delay)
            yield off, self._events[off]


def worker():
    events = dataset()
    backend = MemoryStateBackend()                 # in-memory working state
    store = CheckpointStore(LsmdbStateBackend(CKPT))   # durable checkpoints
    runner = StreamRunner(Pipeline(KeyedAggregate(backend)),
                          store=store, checkpoint_every=8)
    runner.run(SlowSource(events, DELAY), final_checkpoint=True)
    # only reached on clean completion:
    totals = {k: json.loads(backend.get(k).decode()) for k in ("a", "b", "c", "d")}
    with open(RESULT, "w") as f:
        json.dump({"restored_from": runner.restored_from, "totals": totals}, f)
    store.close()
    print("worker: completed cleanly")


def driver():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    truth = ground_truth(dataset())

    print("=" * 60)
    print("Exactly-once crash-recovery demo")
    print("=" * 60)
    print(f"dataset: {N} events, 4 keys; ground-truth totals:\n  {truth}")

    # 1) start worker, let it make a few checkpoints, then SIGKILL it
    p = subprocess.Popen([sys.executable, __file__, "__worker__"])
    time.sleep(1.2)                                # ~60 events processed, several checkpoints
    p.send_signal(signal.SIGKILL)
    p.wait()
    killed_clean = not os.path.exists(RESULT)
    print(f"\n[CRASH] SIGKILL sent mid-stream (pid {p.pid}); "
          f"no result written yet: {killed_clean}")
    assert killed_clean, "worker wrote results before the kill -- increase N/DELAY"

    # 2) fresh worker restores from the durable checkpoint and finishes
    print("[RECOVER] restarting worker; it restores from the last checkpoint...")
    subprocess.run([sys.executable, __file__, "__worker__"], check=True)

    # 3) verify exactly-once
    with open(RESULT) as f:
        res = json.load(f)
    print(f"\nrecovered: resumed after checkpoint offset {res['restored_from']}")
    print(f"final totals: {res['totals']}")
    ok = res["totals"] == truth
    print("\n" + ("PASS  no loss, no double count -- exactly once."
                  if ok else "FAIL  totals differ from ground truth!"))
    print("=" * 60)
    shutil.rmtree(WORK, ignore_errors=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "__worker__":
        worker()
    else:
        driver()
