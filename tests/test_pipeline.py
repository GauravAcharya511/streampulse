from streampulse import Event, ReplayableSource, gen_events, Map, Filter, Pipeline


def test_event_with_value():
    e = Event("a", 1, 100)
    assert e.with_value(5) == Event("a", 5, 100)
    assert e.event_time == 100


def test_map_operator():
    src = ReplayableSource([Event("a", 1, 0), Event("b", 2, 1)])
    out = Pipeline(Map(lambda e: e.with_value(e.value * 10))).run(src)
    assert [e.value for e in out] == [10, 20]


def test_filter_operator():
    src = ReplayableSource([Event("a", 1, 0), Event("b", 2, 1), Event("c", 3, 2)])
    out = Pipeline(Filter(lambda e: e.value % 2 == 1)).run(src)
    assert [e.value for e in out] == [1, 3]


def test_chained_operators():
    src = ReplayableSource([Event("a", i, i) for i in range(5)])
    out = Pipeline(
        Filter(lambda e: e.value % 2 == 0),      # 0,2,4
        Map(lambda e: e.with_value(e.value + 100)),
    ).run(src)
    assert [e.value for e in out] == [100, 102, 104]


def test_replay_from_offset():
    events = [Event("a", i, i) for i in range(5)]
    src = ReplayableSource(events)
    out = Pipeline(Map(lambda e: e)).run(src, start_offset=2)
    assert [e.value for e in out] == [2, 3, 4]


def test_run_tracked_returns_last_offset():
    src = ReplayableSource([Event("a", i, i) for i in range(4)])
    out, last = Pipeline(Map(lambda e: e)).run_tracked(src)
    assert last == 3
    assert len(out) == 4
    # empty tail: nothing to process past the end
    out2, last2 = Pipeline(Map(lambda e: e)).run_tracked(src, start_offset=4)
    assert out2 == [] and last2 == 3


def test_gen_events_shape_and_determinism():
    a = gen_events(100, seed=42)
    b = gen_events(100, seed=42)
    assert a == b                             # deterministic for a given seed
    assert len(a) == 100
    assert all(isinstance(e, Event) for e in a)


def test_gen_events_out_of_order_when_skewed():
    ev = gen_events(200, max_skew_ms=10, seed=1)
    times = [e.event_time for e in ev]
    assert any(times[i] > times[i + 1] for i in range(len(times) - 1))  # some disorder
