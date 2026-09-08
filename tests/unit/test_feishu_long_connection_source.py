from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Mapping

import pytest

from trpc_service.channels.feishu import (
    BlockingFeishuClient,
    LarkSdkLongConnectionSource,
)


class FakeSdkClient:
    def __init__(self, receive: Callable[[object], None]) -> None:
        self._receive = receive
        self.closed = threading.Event()
        self.constructed_on = threading.get_ident()
        self.started_on: int | None = None

    def start(self) -> None:
        self.started_on = threading.get_ident()
        self._receive(
            {
                "header": {
                    "event_id": "evt-1",
                    "event_type": "im.message.receive_v1",
                    "app_id": "cli-test",
                },
                "event": {"message": {"content": json.dumps({"text": "hello"})}},
            }
        )
        self.closed.wait()

    def close(self) -> None:
        self.closed.set()


@pytest.mark.asyncio
async def test_sdk_source_hands_a_provider_event_to_the_async_gateway_and_closes() -> None:
    clients: list[FakeSdkClient] = []

    def factory(receive: Callable[[object], None]) -> BlockingFeishuClient:
        client = FakeSdkClient(receive)
        clients.append(client)
        return client

    received: list[dict[str, object]] = []
    delivered = asyncio.Event()

    async def on_event(event: Mapping[str, object]) -> None:
        received.append(dict(event))
        delivered.set()

    source = LarkSdkLongConnectionSource(
        app_id="cli-test", app_secret="not-logged", client_factory=factory
    )
    task = asyncio.create_task(source.consume("cli-test", on_event))
    try:
        await asyncio.wait_for(delivered.wait(), timeout=5)
    finally:
        await source.close()
    await asyncio.wait_for(task, timeout=5)

    assert received == [
        {
            "header": {
                "event_id": "evt-1",
                "event_type": "im.message.receive_v1",
                "app_id": "cli-test",
            },
            "event": {"message": {"content": '{"text": "hello"}'}},
        }
    ]
    assert clients[0].closed.is_set()
    assert clients[0].started_on == clients[0].constructed_on
