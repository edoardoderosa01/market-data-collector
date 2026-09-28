from decimal import Decimal


class OrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}

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

    @staticmethod
    def _update_side(side, price, qty):
        """
        Update the order book with a new event from the WebSocket stream.
        """
        price, qty = Decimal(price), Decimal(qty)
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    def update(self, event):
        """
        Update the order book with a new event from the WebSocket stream.
        """
        for price, qty in event["b"]:
            self._update_side(self.bids, price, qty)
        for price, qty in event["a"]:
            self._update_side(self.asks, price, qty)
