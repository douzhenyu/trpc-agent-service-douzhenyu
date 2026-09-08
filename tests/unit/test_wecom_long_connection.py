from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trpc_service.channels.wecom_long_connection import (
    MAX_WECOM_REPLY_BYTES,
    WeComLongConnectionClient,
    _reply_within_limit,
)


class FakeWebSocket:
    def __init__(self, frames: list[dict[str, object]]) -> None:
        self._frames = [json.dumps(frame) for frame in frames]
        self.sent: list[dict[str, object]] = []

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))

    def __aiter__(self) -> AsyncIterator[str]:
        return self

    async def __anext__(self) -> str:
        if self._frames:
            return self._frames.pop(0)
        raise asyncio.CancelledError


class ClosingWebSocket(FakeWebSocket):
    async def __anext__(self) -> str:
        if self._frames:
            return self._frames.pop(0)
        raise StopAsyncIteration


class FakeConnection:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, *_: object) -> None:
        return None


class FailingConnection:
    async def __aenter__(self) -> FakeWebSocket:
        raise OSError("peer unavailable")

    async def __aexit__(self, *_: object) -> None:
        return None


@pytest.mark.asyncio
async def test_long_connection_authenticates_routes_text_and_replies_with_original_request_id() -> (
    None
):
    handled: list[dict[str, str]] = []
    websocket = FakeWebSocket(
        [
            {"headers": {"req_id": "subscribe-1"}, "errcode": 0},
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "incoming-1"},
                "body": {
                    "msgid": "message-1",
                    "aibotid": "bot-1",
                    "chattype": "single",
                    "chatid": "user-1",
                    "from": {"userid": "user-1"},
                    "msgtype": "text",
                    "text": {"content": "hello"},
                },
            },
        ]
    )

    async def handle_text(message: dict[str, str]) -> str:
        handled.append(message)
        return "world"

    client = WeComLongConnectionClient(
        bot_id="bot-1",
        bot_secret="not-logged",
        on_text=handle_text,
        connect=lambda _url: FakeConnection(websocket),
        request_id_factory=lambda: "subscribe-1",
        heartbeat_seconds=0,
    )

    with pytest.raises(asyncio.CancelledError):
        await client.run_connection()

    assert websocket.sent[0] == {
        "cmd": "aibot_subscribe",
        "headers": {"req_id": "subscribe-1"},
        "body": {"bot_id": "bot-1", "secret": "not-logged"},
    }
    assert handled == [
        {
            "request_id": "incoming-1",
            "message_id": "message-1",
            "bot_id": "bot-1",
            "chat_type": "single",
            "chat_id": "user-1",
            "from_user_id": "user-1",
            "text": "hello",
            "response_url": "",
        }
    ]
    assert websocket.sent[1] == {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": "incoming-1"},
        "body": {
            "msgtype": "stream",
            "stream": {"id": "message-1", "content": "world", "finish": True},
        },
    }


@pytest.mark.asyncio
async def test_long_connection_stops_after_bounded_exponential_reconnects() -> None:
    delays: list[float] = []

    async def handle_text(_: dict[str, str]) -> str:
        return "unused"

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    client = WeComLongConnectionClient(
        bot_id="bot-1",
        bot_secret="not-logged",
        on_text=handle_text,
        connect=lambda _url: FailingConnection(),
        reconnect_base_seconds=0.5,
        reconnect_max_seconds=1.0,
        max_reconnect_attempts=2,
        sleep=record_sleep,
    )

    await client.run()

    assert delays == [0.5, 1.0]


@pytest.mark.asyncio
async def test_clean_socket_close_is_backed_off_before_reconnecting() -> None:
    delays: list[float] = []
    websocket = ClosingWebSocket([{"headers": {"req_id": "subscribe-1"}, "errcode": 0}])

    async def handle_text(_: dict[str, str]) -> str:
        return "unused"

    client: WeComLongConnectionClient

    async def record_sleep(delay: float) -> None:
        delays.append(delay)
        await client.close()

    client = WeComLongConnectionClient(
        bot_id="bot-1",
        bot_secret="not-logged",
        on_text=handle_text,
        connect=lambda _url: FakeConnection(websocket),
        request_id_factory=lambda: "subscribe-1",
        heartbeat_seconds=0,
        reconnect_base_seconds=0.5,
        sleep=record_sleep,
    )

    await client.run()

    assert delays == [0.5]


@pytest.mark.asyncio
async def test_server_replacement_event_stops_reconnects() -> None:
    delays: list[float] = []
    websocket = ClosingWebSocket(
        [
            {"headers": {"req_id": "subscribe-1"}, "errcode": 0},
            {
                "cmd": "aibot_event_callback",
                "body": {"event": {"eventtype": "disconnected_event"}},
            },
        ]
    )

    async def handle_text(_: dict[str, str]) -> str:
        return "unused"

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    client = WeComLongConnectionClient(
        bot_id="bot-1",
        bot_secret="not-logged",
        on_text=handle_text,
        connect=lambda _url: FakeConnection(websocket),
        request_id_factory=lambda: "subscribe-1",
        heartbeat_seconds=0,
        sleep=record_sleep,
    )

    await client.run()

    assert client.authenticated is False
    assert delays == []


def test_reply_limit_preserves_valid_utf8() -> None:
    reply = _reply_within_limit("你" * (MAX_WECOM_REPLY_BYTES // 2))

    assert len(reply.encode("utf-8")) <= MAX_WECOM_REPLY_BYTES
    assert reply.endswith("你")


@pytest.mark.asyncio
async def test_long_connection_uses_real_local_websocket_peer() -> None:
    received: list[dict[str, object]] = []
    reply_received = asyncio.Event()

    async def peer(websocket: ServerConnection) -> None:
        subscribe = json.loads(await websocket.recv())
        received.append(subscribe)
        await websocket.send(json.dumps({"headers": {"req_id": "subscribe-1"}, "errcode": 0}))
        await websocket.send(
            json.dumps(
                {
                    "cmd": "aibot_msg_callback",
                    "headers": {"req_id": "incoming-1"},
                    "body": {
                        "msgid": "message-1",
                        "aibotid": "bot-1",
                        "chattype": "single",
                        "chatid": "user-1",
                        "from": {"userid": "user-1"},
                        "msgtype": "text",
                        "text": {"content": "hello"},
                    },
                }
            )
        )
        received.append(json.loads(await websocket.recv()))
        reply_received.set()

    async def handle_text(_: dict[str, str]) -> str:
        return "world"

    async with serve(peer, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = WeComLongConnectionClient(
            bot_id="bot-1",
            bot_secret="not-logged",
            on_text=handle_text,
            url=f"ws://127.0.0.1:{port}",
            request_id_factory=lambda: "subscribe-1",
        )
        task = asyncio.create_task(client.run_connection())
        await asyncio.wait_for(reply_received.wait(), timeout=1)
        await client.close()
        await task

    assert received[0]["cmd"] == "aibot_subscribe"
    assert received[1]["headers"] == {"req_id": "incoming-1"}
