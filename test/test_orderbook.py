from decimal import Decimal

import pytest

from market_data_collector.orderbook import OrderBook, OrderBookGapError


def make_book(bids=(), asks=(), last_update_id=100):
    book = OrderBook()
    book.load_snapshot(
        {"bids": list(bids), "asks": list(asks), "lastUpdateId": last_update_id}
    )
    return book


def event(first_id, final_id, prev_final_id=None, bids=(), asks=()):
    return {
        "U": first_id,
        "u": final_id,
        "pu": prev_final_id,
        "b": list(bids),
        "a": list(asks),
    }


# --- alignment with the snapshot (first event) ---


def test_event_older_than_snapshot_is_ignored():
    book = make_book(bids=[["10.0", "1"]], last_update_id=100)
    applied = book.apply_update(event(90, 99, bids=[["10.0", "9"]]))
    assert applied is False
    assert book.bids == {Decimal("10.0"): Decimal("1")}
    assert book.last_update_id == 100
    assert book.synced is False


def test_first_event_covering_snapshot_is_applied():
    book = make_book(last_update_id=100)
    applied = book.apply_update(event(95, 105, bids=[["10.0", "1"]]))
    assert applied is True
    assert book.synced is True
    assert book.last_update_id == 105
    assert book.bids == {Decimal("10.0"): Decimal("1")}


def test_first_event_ending_exactly_at_snapshot_is_applied():
    book = make_book(last_update_id=100)
    assert book.apply_update(event(95, 100)) is True


def test_snapshot_older_than_stream_raises():
    book = make_book(last_update_id=100)
    with pytest.raises(OrderBookGapError):
        book.apply_update(event(105, 110))


# --- continuity after alignment (pu) ---


def test_next_event_is_chained_by_pu_not_by_U():
    book = make_book(last_update_id=100)
    book.apply_update(event(95, 105))
    book.apply_update(event(107, 110, prev_final_id=105))
    assert book.last_update_id == 110


def test_pu_mismatch_is_gap_and_leaves_book_unchanged():
    book = make_book(bids=[["10.0", "1"]], last_update_id=100)
    book.apply_update(event(95, 105))
    with pytest.raises(OrderBookGapError):
        book.apply_update(event(107, 110, prev_final_id=104, bids=[["10.0", "5"]]))
    assert book.last_update_id == 105
    assert book.bids == {Decimal("10.0"): Decimal("1")}


def test_new_snapshot_requires_new_alignment():
    book = make_book(last_update_id=100)
    book.apply_update(event(95, 105))
    book.load_snapshot({"bids": [], "asks": [], "lastUpdateId": 200})
    assert book.synced is False
    with pytest.raises(OrderBookGapError):
        book.apply_update(event(205, 210, prev_final_id=200))


# --- level updates ---


def test_quantity_is_absolute_not_increment():
    book = make_book(bids=[["10.0", "1"]], last_update_id=100)
    book.apply_update(event(100, 101, bids=[["10.0", "3"]]))
    assert book.bids[Decimal("10.0")] == Decimal("3")


def test_zero_quantity_deletes_level():
    book = make_book(asks=[["11.0", "2"]], last_update_id=100)
    book.apply_update(event(100, 101, asks=[["11.0", "0"]]))
    assert book.asks == {}


def test_deleting_missing_level_is_harmless():
    book = make_book(last_update_id=100)
    book.apply_update(event(100, 101, asks=[["99.0", "0"]]))
    assert book.asks == {}


def test_price_keys_compare_by_value_not_text():
    book = make_book(bids=[["10.10", "1"]], last_update_id=100)
    book.apply_update(event(100, 101, bids=[["10.1", "0"]]))
    assert book.bids == {}


def test_load_snapshot_replaces_previous_state():
    book = make_book(bids=[["10.0", "1"]], last_update_id=100)
    book.load_snapshot({"bids": [["20.0", "2"]], "asks": [], "lastUpdateId": 200})
    assert book.bids == {Decimal("20.0"): Decimal("2")}
    assert book.last_update_id == 200
