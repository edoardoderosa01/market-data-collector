import asyncio

import pytest

from market_data_collector import websocket_client as wc


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def __aiter__(self):
        for message in self.messages:
            yield message


def test_full_queue_raises_overflow(monkeypatch):
    messages = ['{"u": 1}', '{"u": 2}', '{"u": 3}']
    monkeypatch.setattr(wc.websockets, "connect", lambda url: FakeWebSocket(messages))
    queue = asyncio.Queue(maxsize=2)

    with pytest.raises(wc.QueueOverflowError):
        asyncio.run(wc.connect_to_websocket("ws://fake", queue))
