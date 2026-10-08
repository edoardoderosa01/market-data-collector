from decimal import Decimal


class OrderBookGapError(Exception):
    pass


class OrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.last_update_id = None

    def load_snapshot(self, snapshot):
        """
        Load and substitute the order book snapshot from the REST API.
        """
        self.bids = {
            Decimal(price): Decimal(quantity) for price, quantity in snapshot["bids"]
        }
        self.asks = {
            Decimal(price): Decimal(quantity) for price, quantity in snapshot["asks"]
        }
        self.last_update_id = snapshot["lastUpdateId"]

    @staticmethod
    def _update_side(side, price, qty):
        """
        Update a single level of one side of the order book.
        """
        price, qty = Decimal(price), Decimal(qty)
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def apply_update(self, event, first_event: bool = False) -> None:
        """
        Check for gaps, if none update the order book with a new event from the WebSocket stream.
        """
        if first_event:
            if not (event["U"] <= self.last_update_id + 1 <= event["u"]):
                raise OrderBookGapError(
                    f"snapshot {self.last_update_id} not covered by U={event['U']} and u={event['u']}"
                )
        elif (self.last_update_id + 1) != event["U"]:
            raise OrderBookGapError(
                f"expected U={self.last_update_id + 1}, arrived U={event['U']}"
            )
        for price, qty in event["b"]:
            self._update_side(self.bids, price, qty)
        for price, qty in event["a"]:
            self._update_side(self.asks, price, qty)
        self.last_update_id = event["u"]
