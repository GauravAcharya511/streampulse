# streampulse — a stream processor with exactly-once recovery, built from scratch

An event-time stream processing engine implementing the core ideas behind Apache
Flink: keyed state, **event-time windowing with watermarks**, out-of-order and
late-event handling, and **checkpoint-based exactly-once recovery** — proven with
a real process kill. Written in Python, from first principles.

Durable state and checkpoints are backed by
[**lsmdb**](https://github.com/GauravAcharya511/lsmdb), an LSM-tree storage
engine I also built from scratch. So this is a **streaming layer on top of a
storage engine, both my own** — a small two-system data platform.

## Results

`python benchmark.py 200000` (numbers vary by machine):

| Metric | Result |
|---|---|
| Windowed aggregation throughput | ~168,000 events/sec |
| Checkpointing overhead (durable, every 1000 events) | **2.4%** |
| Recovery from checkpoint | **<1 ms** |
| Exactly-once under real `SIGKILL` | no loss, no double count (`demo_exactly_once.py`) |

## Architecture

```
   events (event-time, possibly out of order)
            │
            ▼
   ┌──────────────────┐  push(event) / flush()
   │  Operator chain  │  Map · Filter · KeyedAggregate ·
   │  (standing,      │  EventTimeTumblingWindow
   │   stateful)      │
   └──────────────────┘
            │ per-(key,window) state
            ▼
   ┌──────────────────┐   in-memory working state
   │  StateBackend    │──────────────────────────────┐
   └──────────────────┘                               │
            │ every N events: snapshot() + offset     │ snapshot()
            ▼                                          ▼
   ┌──────────────────┐  atomic, durable   ┌────────────────────┐
   │  CheckpointStore │───────────────────▶│   lsmdb (my LSM    │
   │  (offset + state)│                     │   storage engine)  │
   └──────────────────┘                     └────────────────────┘

   watermark = max_event_time_seen − max_out_of_orderness
   a window [ws, we) fires when watermark ≥ we; it stays open for
   allowed_lateness (late events re-fire it); later events → side_output.
   crash + restart → StreamRunner restores state from the last checkpoint
   and resumes from offset+1  ⇒  exactly once.
```

## Design decisions

- **Event-time, not processing-time.** Windows and watermarks reason about when
  an event *happened*, so results are correct regardless of arrival order or
  delays. `max_out_of_orderness` sets how long the watermark waits for stragglers.
- **Watermarks drive firing.** A window fires when the watermark passes its end.
  This is the crux of stream processing and the part most people can only
  describe, not implement.
- **Late data has a full lifecycle.** `allowed_lateness` keeps a fired window open
  for a grace period (late events re-fire it with an updated result); anything
  later goes to a **side-output** instead of silently corrupting a closed result.
- **Pluggable state backends** (the Flink pattern): an in-memory dict for speed,
  or lsmdb for durability. Operators serialize state to bytes, so the backend is
  swappable.
- **Exactly-once = in-memory working state + durable checkpoints.** Only
  checkpoints are durable, and each is a single atomic write (offset + all
  operator snapshots). A crash discards everything since the last checkpoint, and
  replay from the checkpointed offset reproduces it exactly — no loss, no double
  count. `demo_exactly_once.py` proves this with a real `SIGKILL`, not a simulated
  one.
- **All operator state lives on the instance** (via `push`/`flush`, not a
  generator frame), which is what makes `snapshot`/`restore` — and therefore
  checkpointing — possible.

## Usage

```python
from streampulse import (Event, ReplayableSource, Pipeline,
                         EventTimeTumblingWindow, MemoryStateBackend)

op = EventTimeTumblingWindow(size=10, backend=MemoryStateBackend(),
                             max_out_of_orderness=2, allowed_lateness=5)
out = Pipeline(op).run(ReplayableSource([
    Event("a", 1, 0), Event("a", 1, 5), Event("a", 1, 12),
]))
# window [0,10) fires when the watermark (12-2=10) reaches its end
```

Durable, recoverable pipeline:

```python
from streampulse import StreamRunner, CheckpointStore, LsmdbStateBackend
store = CheckpointStore(LsmdbStateBackend("ckpt"))
runner = StreamRunner(Pipeline(op), store=store, checkpoint_every=1000)
out = runner.run(source)      # a later run() restores from `store` and resumes
```

## Development

```bash
pip install -e ".[dev,lsmdb]"
pytest -q                      # 35 tests
python benchmark.py            # reproduce the results table
python demo_exactly_once.py    # real SIGKILL crash + exactly-once recovery
```

## Correctness

35 tests: operator semantics, event-time windows and watermark firing,
out-of-orderness tolerance, late firing and side-output, snapshot/restore
round-trips, resumable checkpointing, and exactly-once under single and repeated
mid-stream crashes (deterministic tests, plus the live-kill demo). CI installs
lsmdb from source and runs the full suite, so the two-project integration is
exercised on a clean machine.

## Roadmap

- **v0.1** — event model + replayable source + operator pipeline ✅
- **v0.2** — keyed state backed by lsmdb ✅
- **v0.3** — event-time tumbling windows + watermarks ✅
- **v0.4** — out-of-order / late event handling (allowed lateness) ✅
- **v0.5** — checkpointing: atomic snapshot of state + source offset ✅
- **v0.6** — exactly-once demo: crash mid-stream, restore, no loss / no double-count ✅
- **v0.7** — benchmarks + full README ✅

## Future work

- **Sliding and session windows** (only tumbling today).
- **Barrier-based checkpointing** for multi-operator graphs (current checkpoints
  suit linear pipelines).
- **Exactly-once *sinks*** via a transactional/idempotent output protocol; today
  the exactly-once guarantee is on aggregated state, not external side effects.
- **Incremental checkpoints** (snapshot only changed state) instead of full state.
- **Parallelism** — keyed partitioning across workers.
