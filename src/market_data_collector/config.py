import logging
import time
from pathlib import Path


# logging configuration
def setup_logging(log_file: str = "logs/collector.log") -> None:
    # Garantisce che la cartella logs esista prima di scriverci
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Forza TUTTI i log ad usare l'orario UTC anziché quello locale del PC/Server
    logging.Formatter.converter = time.gmtime

    # La riga di configurazione principale (basicConfig)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",  # Formato ISO-8601 UTC (la Z finale indica UTC)
        handlers=[
            logging.FileHandler(log_file),  # Scrive su file
            logging.StreamHandler(),  # Stampa anche a schermo nel terminale
        ],
    )


# global variables
SYMBOL = "BTCUSDT"
LIMIT = 1000
SPEED = 100  # in ms

REST_URL = "https://api.binance.com/api/v3/depth"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth@{SPEED}ms"
