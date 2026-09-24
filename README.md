# streampulse — a stream processor with exactly-once recovery, built from scratch

An event-time stream processing engine implementing the ideas behind Apache
Flink: keyed state, event-time **windowing with watermarks**, out-of-order event
handling, and **checkpoint-based exactly-once recovery**. Written in Python, from
first principles.

Durable keyed state and checkpoints are backed by
[**lsmdb**](https://github.com/GauravAcharya511/lsmdb), an LSM-tree storage
engine I built — so this project is the streaming layer on top of a storage
engine, both from scratch.

> Status: **v0.1** — event model, replayable offset-addressed source, and a
> composable operator pipeline. Roadmap below.

## Roadmap
- **v0.1** — event model + replayable source + operator pipeline (map/filter) ✅
- **v0.2** — keyed state backed by lsmdb (running per-key aggregation)
- **v0.3** — event-time tumbling windows + watermarks
- **v0.4** — out-of-order / late event handling (allowed lateness)
- **v0.5** — checkpointing: atomic snapshot of state + source offset
- **v0.6** — exactly-once demo: crash mid-stream, restore, no loss / no double-count
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
