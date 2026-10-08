# test/test_orderbook.py
import pytest

from market_data_collector.orderbook import OrderBook, OrderBookGapError


def test_gap_detected():
    book = OrderBook()
    book.load_snapshot({"bids": [], "asks": [], "lastUpdateId": 100})
    fake_event = {"U": 105, "u": 110, "b": [], "a": []}  # salta da 100 a 105: gap
    with pytest.raises(OrderBookGapError):
        book.apply_update(fake_event, first_event=True)
