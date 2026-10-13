import asyncio
from importlib.metadata import entry_points

import pytest

from market_data_collector import main as m
from market_data_collector.orderbook import CrossedBookError, OrderBookGapError
from market_data_collector.rest_client import SnapshotError
from market_data_collector.websocket_client import (
    QueueOverflowError,
    StreamClosedError,
    StreamStaleError,
)


def test_reader_failure_surface_as_stream_closed(monkeypatch):
    """
    Test that if the reader task fails, it raises a StreamClosedError.
    """

    async def failed_reader(url, queue):
        raise ValueError("Socket died")

    monkeypatch.setattr(m, "connect_to_websocket", failed_reader)
    monkeypatch.setattr(
        m,
        "get_snapshot",
        lambda symbol, limit: {"bids": [], "asks": [], "lastUpdateId": 1},
    )
    monkeypatch.setattr(m, "QUEUE_TIMEOUT", 0.1)

    with pytest.raises(StreamClosedError):
        asyncio.run(m.bootstrap_and_consume())


def test_silent_stream_raises_stale(monkeypatch):
    """
    Test that if the reader task is silent, it raises a StreamStaleError
    after QUEUE_TIMEOUT seconds.
    """

    async def silent_reader(url, queue):
        await asyncio.sleep(3600)

    monkeypatch.setattr(m, "connect_to_websocket", silent_reader)
    monkeypatch.setattr(
        m,
        "get_snapshot",
        lambda symbol, limit: {"bids": [], "asks": [], "lastUpdateId": 1},
    )
    monkeypatch.setattr(m, "QUEUE_TIMEOUT", 0.1)

    with pytest.raises(StreamStaleError):
        asyncio.run(m.bootstrap_and_consume())


def test_reader_overflow_keeps_its_type(monkeypatch):
    async def overflowing_reader(url, queue):
        raise QueueOverflowError("Queue full")

    monkeypatch.setattr(m, "connect_to_websocket", overflowing_reader)
    monkeypatch.setattr(
        m,
        "get_snapshot",
        lambda symbol, limit: {"bids": [], "asks": [], "lastUpdateId": 1},
    )
    monkeypatch.setattr(m, "QUEUE_TIMEOUT", 0.1)

    with pytest.raises(QueueOverflowError):
        asyncio.run(m.bootstrap_and_consume())


class StopLoop(Exception):
    """Not handled by main(): the only way for the test to end its infinite loop."""


def test_backoff_grows_then_resets_after_live(monkeypatch):
    script = [
        (False, SnapshotError("binance down")),
        (False, OrderBookGapError("stale snapshot")),
        (True, StreamClosedError("24h disconnect")),
        (False, StopLoop("end of test")),
    ]

    async def fake_bootstrap(symbol, speed, n_events, live):
        went_live, error = script.pop(0)
        if went_live:
            live.set()
        raise error

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(m, "bootstrap_and_consume", fake_bootstrap)
    monkeypatch.setattr(m.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(m.random, "uniform", lambda a, b: 1.0)

    with pytest.raises(StopLoop):
        asyncio.run(m.main())

    assert sleeps == [2, 4, 2]


def make_reader(events):
    async def fake_reader(url, queue):
        for event in events:
            queue.put_nowait((0, 0, event))
        await asyncio.sleep(3600)

    return fake_reader


def test_crossed_book_raises(monkeypatch):
    snapshot = {"bids": [["101", "1"]], "asks": [["100", "1"]], "lastUpdateId": 10}
    events = [{"U": 9, "u": 11, "pu": 8, "b": [], "a": []}]

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", lambda symbol, limit: snapshot)

    with pytest.raises(CrossedBookError):
        asyncio.run(m.bootstrap_and_consume())


def test_old_events_skipped_and_book_goes_live(monkeypatch):
    snapshot = {"bids": [["99", "1"]], "asks": [["100", "1"]], "lastUpdateId": 10}
    events = [
        {"U": 5, "u": 9, "pu": 4, "b": [["98", "1"]], "a": []},
        {"U": 9, "u": 11, "pu": 9, "b": [], "a": []},
    ]
    live = asyncio.Event()

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", lambda symbol, limit: snapshot)

    asyncio.run(m.bootstrap_and_consume(n_events=1, live=live))

    assert live.is_set()


def test_empty_side_does_not_crash_cross_check(monkeypatch):
    snapshot = {"bids": [["99", "1"]], "asks": [], "lastUpdateId": 10}
    events = [{"U": 9, "u": 11, "pu": 8, "b": [], "a": []}]

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", lambda symbol, limit: snapshot)

    asyncio.run(m.bootstrap_and_consume(n_events=1))


def test_crossed_book_stops_main_without_retry(monkeypatch):
    async def fake_bootstrap(symbol, speed, n_events, live):
        raise CrossedBookError("bad book")

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(m, "bootstrap_and_consume", fake_bootstrap)
    monkeypatch.setattr(m.asyncio, "sleep", fake_sleep)

    with pytest.raises(CrossedBookError):
        asyncio.run(m.main())

    assert sleeps == []


def test_old_snapshot_is_refetched_without_reconnecting(monkeypatch):
    snapshots = [
        {"bids": [], "asks": [], "lastUpdateId": 5},
        {"bids": [], "asks": [], "lastUpdateId": 10},
    ]
    calls = []

    def fake_get_snapshot(symbol, limit):
        calls.append(symbol)
        return snapshots.pop(0)

    events = [{"U": 9, "u": 11, "pu": 8, "b": [], "a": []}]
    live = asyncio.Event()

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", fake_get_snapshot)
    monkeypatch.setattr(m, "SNAPSHOT_RETRY_DELAY", 0)

    asyncio.run(m.bootstrap_and_consume(n_events=1, live=live))

    assert len(calls) == 2
    assert live.is_set()


def test_snapshot_never_aligned_raises_snapshot_error(monkeypatch):
    calls = []

    def always_old_snapshot(symbol, limit):
        calls.append(symbol)
        return {"bids": [], "asks": [], "lastUpdateId": 5}

    events = [{"U": 9, "u": 11, "pu": 8, "b": [], "a": []}]

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", always_old_snapshot)
    monkeypatch.setattr(m, "SNAPSHOT_RETRY_DELAY", 0)

    with pytest.raises(SnapshotError):
        asyncio.run(m.bootstrap_and_consume())

    assert len(calls) == m.SNAPSHOT_MAX_TRIES


def test_runs_until_error_when_no_event_limit(monkeypatch):
    snapshot = {"bids": [["99", "1"]], "asks": [["100", "1"]], "lastUpdateId": 10}
    events = [
        {"U": 9, "u": 11, "pu": 8, "b": [], "a": []},
        {"U": 12, "u": 13, "pu": 11, "b": [], "a": []},
        {"U": 14, "u": 15, "pu": 13, "b": [], "a": []},
    ]
    live = asyncio.Event()

    monkeypatch.setattr(m, "connect_to_websocket", make_reader(events))
    monkeypatch.setattr(m, "get_snapshot", lambda symbol, limit: snapshot)
    monkeypatch.setattr(m, "QUEUE_TIMEOUT", 0.1)

    with pytest.raises(StreamStaleError):
        asyncio.run(m.bootstrap_and_consume(live=live))

    assert live.is_set()


def test_console_script_points_to_run():
    (entry_point,) = entry_points(group="console_scripts", name="market-data-collector")
    assert entry_point.load() is m.run
