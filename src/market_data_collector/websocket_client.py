import asyncio
import json
from datetime import UTC, datetime

import websockets


async def connect_to_websocket(url: str, queue: asyncio.Queue) -> None:
    """
    Read from WS and puts (receive_time, message) into the queue.
    """
    async with websockets.connect(url) as ws:
        async for message in ws:
            recv_time = datetime.now(UTC)
            await queue.put((recv_time, json.loads(message)))
