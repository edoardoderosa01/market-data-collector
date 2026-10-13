import asyncio
import logging
import random

from market_data_collector.config import (
    LIMIT,
    SPEED,
    SYMBOL,
    build_ws_url,
    setup_logging,
)
from market_data_collector.orderbook import (
    CrossedBookError,
    OrderBook,
    OrderBookError,
)
from market_data_collector.rest_client import SnapshotError, get_snapshot
from market_data_collector.websocket_client import (
    StreamClosedError,
    StreamError,
    StreamStaleError,
    connect_to_websocket,
)

logger = logging.getLogger(__name__)

CROSS_CHECK_EVERY = 500
# max seconds without any message before the stream is considered dead
QUEUE_TIMEOUT = 10
MAX_BACKOFF = 60
QUEUE_MAXSIZE = 1000
SNAPSHOT_MAX_TRIES = 3
SNAPSHOT_RETRY_DELAY = 1  # seconds between snapshot fetches


async def next_message(
    queue: asyncio.Queue, reader_task: asyncio.Task
) -> tuple[int, int, dict]:
    """
    Return the next buffered (recv_ts_ns, recv_mono_ns, event).
    Raise a StreamError if the reader died or the stream went silent.
    """
    try:
        return await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)
    except TimeoutError:
        if reader_task.done():
            exc = reader_task.exception()
            if isinstance(exc, StreamError):
                raise exc
            raise StreamClosedError("Websocket stream ended") from exc
        raise StreamStaleError(
            f"No data received for {QUEUE_TIMEOUT} seconds, stream is stale"
        )


async def fetch_aligned_snapshot(symbol: str, first_update_id: int) -> dict:
    """
    Fetch a REST snapshot that is not older than the first buffered event.
    Only the snapshot is re-fetched; the WebSocket keeps buffering meanwhile.
    """
    for attempt in range(1, SNAPSHOT_MAX_TRIES + 1):
        snapshot = await asyncio.to_thread(get_snapshot, symbol, LIMIT)
        if snapshot["lastUpdateId"] >= first_update_id:
            return snapshot
        logger.info(
            f"Snapshot {snapshot['lastUpdateId']} older than first event "
            f"U={first_update_id}, re-fetching ({attempt}/{SNAPSHOT_MAX_TRIES})"
        )
        await asyncio.sleep(SNAPSHOT_RETRY_DELAY)
    raise SnapshotError(
        f"Snapshot still older than first event U={first_update_id} "
        f"after {SNAPSHOT_MAX_TRIES} tries"
    )


async def bootstrap_and_consume(
    symbol: str = SYMBOL,
    speed: int = SPEED,
    n_events: int | None = None,
    live: asyncio.Event | None = None,
) -> None:
    """
    Bootstrap the order book and consume events from the WebSocket.
    Runs until an error is raised; n_events stops it early (tests only).
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    reader_task = asyncio.create_task(
        connect_to_websocket(build_ws_url(symbol, speed), queue)
    )

    try:
        msg = await next_message(queue, reader_task)
        _, _, first_event = msg
        snapshot = await fetch_aligned_snapshot(symbol, first_event["U"])
        logger.info(f"Snapshot fetched, lastUpdateId={snapshot['lastUpdateId']}")

        book = OrderBook()
        book.load_snapshot(snapshot)
        processed = 0

        while True:
            recv_ts_ns, recv_mono_ns, event = msg
            if book.apply_update(event):
                if (
                    book.bids
                    and book.asks
                    and processed % CROSS_CHECK_EVERY == 0
                    and max(book.bids) >= min(book.asks)
                ):
                    raise CrossedBookError(
                        f"Crossed book detected: max bid {max(book.bids)} >= min ask {min(book.asks)}"
                    )
                if processed == 0 and live is not None:
                    live.set()
                processed += 1
                if n_events is not None and processed >= n_events:
                    return
            msg = await next_message(queue, reader_task)
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Reader task ended with {e!r}")


async def main(
    symbol: str = SYMBOL, speed: int = SPEED, n_events: int | None = None
) -> None:
    """
    Main function to run the market data collector.
    """
    attempts = 0
    while True:
        live = asyncio.Event()
        try:
            await bootstrap_and_consume(symbol, speed, n_events, live=live)
        except CrossedBookError as e:
            logger.critical(f"{type(e).__name__}: {e}, logic error, stopping")
            raise
        except (StreamError, SnapshotError, OrderBookError) as e:
            if live.is_set():
                attempts = 0
            attempts += 1
            backoff = min(2**attempts, MAX_BACKOFF) * random.uniform(0.5, 1.5)
            logger.warning(
                f"{type(e).__name__}: {e} (cause: {e.__cause__!r}), retrying in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)


def run() -> None:
    """
    Entry point for the market data collector.
    """
    setup_logging()
    asyncio.run(main())


if __name__ == "__main__":
    run()
