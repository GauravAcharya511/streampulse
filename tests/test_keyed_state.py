"""v0.2 -- keyed state and stateful aggregation, over both state backends."""
import pytest
from streampulse import (
    Event, ReplayableSource, Pipeline, KeyedAggregate, RunningCountSum,
    MemoryStateBackend,
)


def _events():
    return [
        Event("a", 10, 0),
        Event("b", 5, 1),
        Event("a", 20, 2),
        Event("b", 7, 3),
        Event("a", 1, 4),
    ]


def test_running_count_sum_memory():
    src = ReplayableSource(_events())
    out = Pipeline(KeyedAggregate(MemoryStateBackend())).run(src)
    # per-key running aggregates in arrival order
    assert out[0].value == {"count": 1, "sum": 10}   # a
    assert out[1].value == {"count": 1, "sum": 5}    # b
    assert out[2].value == {"count": 2, "sum": 30}   # a
    assert out[3].value == {"count": 2, "sum": 12}   # b
    assert out[4].value == {"count": 3, "sum": 31}   # a


def test_state_isolated_per_key():
    src = ReplayableSource([Event("x", 1, 0), Event("y", 100, 1), Event("x", 1, 2)])
    out = Pipeline(KeyedAggregate(MemoryStateBackend())).run(src)
    assert out[0].value == {"count": 1, "sum": 1}
    assert out[1].value == {"count": 1, "sum": 100}
    assert out[2].value == {"count": 2, "sum": 2}


def test_aggregate_chains_with_stateless_ops():
    from streampulse import Filter
    src = ReplayableSource(_events())
    # only key 'a', then aggregate
    out = Pipeline(
        Filter(lambda e: e.key == "a"),
        KeyedAggregate(MemoryStateBackend()),
    ).run(src)
    assert [o.value["sum"] for o in out] == [10, 30, 31]


def test_state_backend_get_put_roundtrip():
    b = MemoryStateBackend()
    assert b.get("k") is None
    b.put("k", b"hello")
    assert b.get("k") == b"hello"


# ---- lsmdb-backed backend (durable): skipped if lsmdb isn't installed ----
def test_lsmdb_backend_persists_across_restart(tmp_path):
    lsmdb = pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend

    path = str(tmp_path / "state")
    b1 = LsmdbStateBackend(path)
    src = ReplayableSource(_events())
    out1 = Pipeline(KeyedAggregate(b1)).run(src)
    assert out1[-1].value == {"count": 3, "sum": 31}   # key 'a'
    b1.close()

    # reopen the SAME state dir: aggregation continues from persisted state
    b2 = LsmdbStateBackend(path)
    out2 = Pipeline(KeyedAggregate(b2)).run(ReplayableSource([Event("a", 4, 5)]))
    assert out2[0].value == {"count": 4, "sum": 35}    # state survived restart
    b2.close()


def test_memory_and_lsmdb_agree(tmp_path):
    pytest.importorskip("lsmdb")
    from streampulse import LsmdbStateBackend
    ev = _events()
    mem = Pipeline(KeyedAggregate(MemoryStateBackend())).run(ReplayableSource(ev))
    lsm_backend = LsmdbStateBackend(str(tmp_path / "s"))
    lsm = Pipeline(KeyedAggregate(lsm_backend)).run(ReplayableSource(ev))
    assert [o.value for o in mem] == [o.value for o in lsm]
    lsm_backend.close()
