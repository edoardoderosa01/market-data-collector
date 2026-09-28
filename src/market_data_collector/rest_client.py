import requests

from market_data_collector.config import LIMIT, REST_URL, SYMBOL


def get_snapshot(symbol=SYMBOL, limit=LIMIT):
    """
    Fetch the order book snapshot from the REST API.
    """
    params = {"symbol": symbol, "limit": limit}
    url = REST_URL
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()
