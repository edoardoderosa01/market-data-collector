import requests

from market_data_collector.config import REST_URL


def get_snapshot(symbol: str, limit: int) -> dict:
    """
    Fetch the order book snapshot from the REST API.
    """
    params = {"symbol": symbol, "limit": limit}
    url = REST_URL
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
