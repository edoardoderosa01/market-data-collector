import logging
import time
from pathlib import Path


# logging configuration
def setup_logging(log_file: str = "logs/collector.log") -> None:

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.Formatter.converter = time.gmtime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


# global variables
SYMBOL = "BTCUSDT"
LIMIT = 1000
SPEED = 100  # in ms

REST_URL = "https://api.binance.com/api/v3/depth"


def build_ws_url(symbol: str = SYMBOL, speed: int = SPEED) -> str:
    return f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth@{speed}ms"
