import requests

from market_data_collector.config import REST_URL

REST_TIMEOUT = 10  # seconds: max wait to connect, and max wait between bytes


class SnapshotError(Exception):
    """Error fetching snapshot from REST API"""


def get_snapshot(symbol: str, limit: int) -> dict:
    """
    Fetch the order book snapshot from the REST API.
    """
    params = {"symbol": symbol, "limit": limit}
    url = REST_URL
    try:
        response = requests.get(url, params=params, timeout=REST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise SnapshotError(f"Snapshot fetch failed for {symbol}: {e!r}") from e
