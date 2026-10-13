from decimal import Decimal


class OrderBookError(Exception):
    """Base class for order book errors"""


class OrderBookGapError(OrderBookError):
    """Raised when a gap is detected in the order book updates"""


class CrossedBookError(OrderBookError):
    """Raised when the order book becomes crossed"""


class OrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.last_update_id = None
        self.synced = False

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
        self.synced = False

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

    def apply_update(self, event) -> bool:
        """
        Apply one USDⓈ-M futures diff-depth event (Binance local order book rules).
        Return False if the event is older than the snapshot (ignored).
        Raise OrderBookGapError if the event does not continue the book.
        """
        if not self.synced:
            if event["u"] < self.last_update_id:
                return False
            if not (event["U"] <= self.last_update_id <= event["u"]):
                raise OrderBookGapError(
                    f"first event U={event['U']} u={event['u']} "
                    f"does not cover snapshot {self.last_update_id}"
                )
        elif event["pu"] != self.last_update_id:
            raise OrderBookGapError(
                f"expected pu={self.last_update_id}, got pu={event['pu']}"
            )
        for price, qty in event["b"]:
            self._update_side(self.bids, price, qty)
        for price, qty in event["a"]:
            self._update_side(self.asks, price, qty)
        self.last_update_id = event["u"]
        self.synced = True
        return True
