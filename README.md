# streampulse — a stream processor with exactly-once recovery, built from scratch

An event-time stream processing engine implementing the ideas behind Apache
Flink: keyed state, event-time **windowing with watermarks**, out-of-order event
handling, and **checkpoint-based exactly-once recovery**. Written in Python, from
first principles.

Durable keyed state and checkpoints are backed by
[**lsmdb**](https://github.com/GauravAcharya511/lsmdb), an LSM-tree storage
engine I built — so this project is the streaming layer on top of a storage
engine, both from scratch.

> Status: **v0.6** — exactly-once crash recovery, demonstrated with a real
> process kill (SIGKILL). Roadmap below.

## Roadmap
- **v0.1** — event model + replayable source + operator pipeline (map/filter) ✅
- **v0.2** — keyed state backed by lsmdb (running per-key aggregation) ✅
- **v0.3** — event-time tumbling windows + watermarks ✅
- **v0.4** — out-of-order / late event handling (allowed lateness) ✅
- **v0.5** — checkpointing: atomic snapshot of state + source offset ✅
- **v0.6** — exactly-once demo: crash mid-stream, restore, no loss / no double-count ✅
- **v0.7** — benchmarks + full README

## Usage (v0.1)
```python
from streampulse import Event, ReplayableSource, Map, Filter, Pipeline

src = ReplayableSource([Event("a", 1, 0), Event("b", 2, 1), Event("c", 3, 2)])
out = Pipeline(
    Filter(lambda e: e.value % 2 == 1),
    Map(lambda e: e.with_value(e.value * 10)),
).run(src)
# [Event('a', 10, 0), Event('c', 30, 2)]
```

## Development
```bash
pip install -e ".[dev]"
pytest -q
```

## Keyed state (v0.2)

Stateful aggregation with pluggable state backends -- an in-memory dict, or
**lsmdb** for durable state that survives restarts:

```python
from streampulse import (ReplayableSource, Pipeline, KeyedAggregate,
                         LsmdbStateBackend, Event)

backend = LsmdbStateBackend("state_dir")          # persisted in lsmdb
src = ReplayableSource([Event("a", 10, 0), Event("a", 20, 1)])
out = Pipeline(KeyedAggregate(backend)).run(src)
# out[-1].value == {"count": 2, "sum": 30}  -- and it survives a restart
backend.close()
```

Install the lsmdb backend:  `pip install -e ".[lsmdb]"`

## Event-time windows (v0.3)

Tumbling windows keyed per event, fired by a watermark once event-time passes the
window's end. `max_out_of_orderness` sets how long to wait for stragglers; events
arriving after their window fires are counted as late.

```python
from streampulse import (ReplayableSource, Pipeline, EventTimeTumblingWindow,
                         MemoryStateBackend, Event)

op = EventTimeTumblingWindow(size=10, backend=MemoryStateBackend(),
                             max_out_of_orderness=2)
out = Pipeline(op).run(ReplayableSource([
    Event("a", 1, 0), Event("a", 1, 5), Event("a", 1, 12),
]))
# window [0,10) fires when the watermark (12-2=10) reaches its end:
#   {"count": 2, "sum": 2, "window_start": 0, "window_end": 10}
print(op.late_count)   # events dropped for arriving after their window fired
```

## Late data (v0.4)

`allowed_lateness` keeps a window open past its fire time; a late-but-allowed
event re-fires the window with an updated result. Events later than that are
routed to `side_output` instead of silently corrupting a closed result.

```python
op = EventTimeTumblingWindow(size=10, backend=MemoryStateBackend(),
                             allowed_lateness=10)
out = Pipeline(op).run(ReplayableSource([
    Event("a", 1, 0), Event("a", 1, 10),   # fires window [0,10) with count 1
    Event("a", 1, 15), Event("a", 1, 4),   # t=4 is late-but-allowed -> re-fires, count 2
]))
op.side_output   # [] here; would hold events too late for the grace period
```

## Checkpointing (v0.5)

`StreamRunner` snapshots every operator's state plus the source offset every N
events and commits it atomically to a durable `CheckpointStore` (backed by
lsmdb). After a crash, a fresh runner restores that state and resumes from the
offset after the last checkpoint -- producing results identical to an
uninterrupted run.

```python
from streampulse import (StreamRunner, CheckpointStore, LsmdbStateBackend,
                         Pipeline, EventTimeTumblingWindow, MemoryStateBackend)

store = CheckpointStore(LsmdbStateBackend("ckpt"))          # durable
op = EventTimeTumblingWindow(10, MemoryStateBackend())      # in-memory working state
runner = StreamRunner(Pipeline(op), store=store, checkpoint_every=1000)
out = runner.run(source)     # a later run() restores from `store` and resumes
```

Working state is in-memory; only checkpoints are durable. So a crash discards
everything since the last checkpoint and replay reproduces it exactly -- the
basis for the exactly-once recovery demo in v0.6.

## Exactly-once crash recovery (v0.6)

`demo_exactly_once.py` proves the headline property with a **real crash**: it
streams a fixed dataset in a worker subprocess, `SIGKILL`s it mid-stream, then
restarts a fresh worker that restores from the last durable checkpoint and
finishes. Final per-key totals equal ground truth -- no event lost, none counted
twice.

```
$ python demo_exactly_once.py
[CRASH]   SIGKILL sent mid-stream; no result written yet: True
[RECOVER] restarting worker; it restores from the last checkpoint...
recovered: resumed after checkpoint offset 55
PASS  no loss, no double count -- exactly once.
```

Why it holds: working state is in-memory and only checkpoints are durable, so a
crash discards everything since the last checkpoint and replay from the
checkpointed offset reproduces it exactly. Requires the lsmdb backend
(`pip install -e ".[lsmdb]"`).
