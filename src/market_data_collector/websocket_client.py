import asyncio
import json
import time

import websockets


class StreamError(Exception):
    """Websocket stream stopped delivering data, resync needed"""


class StreamClosedError(StreamError):
    """Reader task ended (connection closed or failed)"""


class StreamStaleError(StreamError):
    """Connection open but no data received for a long time"""


class QueueOverflowError(StreamError):
    """Queue full, consumer too slow"""


async def connect_to_websocket(url: str, queue: asyncio.Queue) -> None:
    """
    Read from WS and puts (recv_ts_ns, recv_mono_ns, message) into the queue.
    """
    async with websockets.connect(url) as ws:
        async for message in ws:
            recv_ts_ns = time.time_ns()
            recv_mono_ns = time.monotonic_ns()
            parsed_msg = json.loads(message)
            try:
                queue.put_nowait((recv_ts_ns, recv_mono_ns, parsed_msg))
            except asyncio.QueueFull as e:
                raise QueueOverflowError(
                    f"Queue full ({queue.maxsize} items), consumer too slow"
                ) from e
