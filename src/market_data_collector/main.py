import asyncio
import logging

from market_data_collector.config import (
    LIMIT,
    SPEED,
    SYMBOL,
    build_ws_url,
    setup_logging,
)
from market_data_collector.orderbook import OrderBook, OrderBookGapError
from market_data_collector.rest_client import get_snapshot
from market_data_collector.websocket_client import connect_to_websocket

logger = logging.getLogger(__name__)

CROSS_CHECK_EVERY = 500
QUEUE_TIMEOUT = 10
MAX_BACKOFF = 60


async def bootstrap_and_consume(
    symbol: str = SYMBOL, speed: int = SPEED, n_events: int = 1000
) -> OrderBook:
    """
    Main function to run the market data collector.
    """
    queue: asyncio.Queue = asyncio.Queue()
    reader_task = asyncio.create_task(
        connect_to_websocket(build_ws_url(symbol, speed), queue)
    )

    try:
        await asyncio.sleep(1)
        snapshot = await asyncio.to_thread(get_snapshot, symbol, LIMIT)
        logger.info(f"Snapshot fetched, LastUpdatedId={snapshot['LastUpdateId']}")

        book = OrderBook()
        book.load_snapshot(snapshot)

        first = True
        processed = 0

        while processed < n_events:
            try:
                recv_time, event = await asyncio.wait_for(
                    queue.get(), timeout=QUEUE_TIMEOUT
                )
            except TimeoutError:
                if reader_task.done():
                    exc = reader_task.exception()
                    raise RuntimeError("WebSocket reader task failed") from exc
                continue
            if event["u"] <= book.last_update_id:
                continue  # snapshot already fully cover this event
            book.apply_update(event, first_event=first)
            if processed % CROSS_CHECK_EVERY == 0 and max(book.bids) >= min(book.asks):
                raise OrderBookGapError(
                    f"Gap detected: max bid {max(book.bids)} >= min ask {min(book.asks)}"
                )
            first = False
            processed += 1
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass


async def main(symbol: str = SYMBOL, speed: int = SPEED, n_events: int = 1000) -> None:
    """
    Main function to run the market data collector.
    """
    attempts = 0
    while True:
        try:
            await bootstrap_and_consume(symbol, speed, n_events)
        except OrderBookGapError as e:
            logger.warning("Gap detected in order book: %s", e)
            await asyncio.sleep(1)
        except RuntimeError as e:
            attempts += 1
            backoff = min(2**attempts, MAX_BACKOFF)
            logger.critical("Connection error: %s", e)
            await asyncio.sleep(backoff)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
