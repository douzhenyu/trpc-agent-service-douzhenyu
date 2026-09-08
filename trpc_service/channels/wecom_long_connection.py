"""企业微信智能机器人长连接协议适配器。

The adapter owns only the native WebSocket protocol.  It deliberately passes a
normalised text event to the Channel Gateway, which remains responsible for
tenant binding, durable idempotency, release routing and agent execution.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from types import TracebackType
from typing import Any, Protocol
from uuid import uuid4

from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosed

DEFAULT_WECOM_WEBSOCKET_URL = "wss://openws.work.weixin.qq.com"
MAX_WECOM_REPLY_BYTES = 20_480
_LOGGER = logging.getLogger("uvicorn.error")


class WebSocketConnection(Protocol):
    async def send(self, data: str) -> None: ...

    async def close(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...


class WebSocketContext(Protocol):
    async def __aenter__(self) -> WebSocketConnection: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


ConnectionFactory = Callable[[str], WebSocketContext]
TextHandler = Callable[[dict[str, str]], Awaitable[str | None]]


class WeComLongConnectionClient:
    """Maintains one authenticated WeCom smart-bot WebSocket connection.

    Raw frames and credentials are intentionally never logged.  The callback
    result is sent as the final stream chunk using the inbound frame's request
    id, as required by the WeCom protocol.
    """

    def __init__(
        self,
        *,
        bot_id: str,
        bot_secret: str,
        on_text: TextHandler,
        url: str = DEFAULT_WECOM_WEBSOCKET_URL,
        connect: ConnectionFactory | None = None,
        request_id_factory: Callable[[], str] | None = None,
        heartbeat_seconds: float = 30.0,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        max_reconnect_attempts: int = 10,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._bot_id = bot_id
        self._bot_secret = bot_secret
        self._on_text = on_text
        self._url = url
        # Enterprise runtimes often export a SOCKS proxy for development tools.
        # WeCom's long-connection endpoint must be a direct outbound WSS flow;
        # proxy auto-discovery can otherwise make the adapter fail before auth.
        self._connect = connect or (lambda endpoint: websocket_connect(endpoint, proxy=None))
        self._request_id = request_id_factory or (lambda: str(uuid4()))
        self._heartbeat_seconds = heartbeat_seconds
        self._reconnect_base_seconds = reconnect_base_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._max_reconnect_attempts = max_reconnect_attempts
        self._sleep = sleep
        self._closed = False
        self._authenticated = False
        self._socket: WebSocketConnection | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    async def run(self) -> None:
        """Run until explicitly closed or a bounded reconnect budget is exhausted."""

        attempts = 0
        while not self._closed:
            try:
                await self.run_connection()
                attempts = 0
            except asyncio.CancelledError:
                raise
            except Exception as error:
                close_code = getattr(getattr(error, "rcvd", None), "code", None)
                _LOGGER.warning(
                    "wecom_long_connection_disconnected error_type=%s close_code=%s",
                    type(error).__name__,
                    close_code,
                )
            if self._closed:
                return
            attempts += 1
            if attempts > self._max_reconnect_attempts:
                return
            delay = min(
                self._reconnect_base_seconds * (2 ** (attempts - 1)),
                self._reconnect_max_seconds,
            )
            await self._sleep(delay)

    async def run_connection(self) -> None:
        """Authenticate and service a single physical connection.

        Kept public so a deterministic local WebSocket peer can exercise the
        protocol without reaching the public WeCom endpoint.
        """

        subscription_request_id = self._request_id()
        async with self._connect(self._url) as socket:
            self._socket = socket
            await self._send(
                {
                    "cmd": "aibot_subscribe",
                    "headers": {"req_id": subscription_request_id},
                    "body": {"bot_id": self._bot_id, "secret": self._bot_secret},
                }
            )
            try:
                async for raw_frame in socket:
                    await self._handle_frame(raw_frame, subscription_request_id)
            except ConnectionClosed:
                if not self._closed:
                    raise
            finally:
                await self._stop_heartbeat()
                self._socket = None
                self._authenticated = False

    async def close(self) -> None:
        self._closed = True
        await self._stop_heartbeat()
        if self._socket is not None:
            close_task = asyncio.create_task(self._socket.close())
            done, _ = await asyncio.wait({close_task}, timeout=2.0)
            if done:
                with suppress(Exception):
                    close_task.result()
            elif isinstance(self._socket, ClientConnection):
                # Some peers omit the close frame. Abort their TCP transport so
                # application shutdown is still bounded.
                self._socket.transport.abort()

    async def _handle_frame(self, raw_frame: str | bytes, subscription_request_id: str) -> None:
        if isinstance(raw_frame, bytes):
            raw_frame = raw_frame.decode("utf-8")
        try:
            frame = json.loads(raw_frame)
        except (TypeError, ValueError, UnicodeDecodeError):
            return
        if not isinstance(frame, dict):
            return
        headers = frame.get("headers")
        if isinstance(headers, dict) and headers.get("req_id") == subscription_request_id:
            _LOGGER.info("wecom_long_connection_subscription_response_received")
        if self._is_subscription_ack(frame, subscription_request_id):
            self._authenticated = True
            self._start_heartbeat()
            _LOGGER.info("wecom_long_connection_authenticated")
            return
        if frame.get("cmd") == "aibot_event_callback":
            body = frame.get("body")
            event = body.get("event") if isinstance(body, dict) else None
            event_type = event.get("eventtype") if isinstance(event, dict) else None
            if event_type == "disconnected_event":
                # WeCom explicitly tells the displaced connection that another
                # owner has connected. Reconnecting here only evicts the new
                # owner in turn, creating an avoidable reconnect storm.
                self._closed = True
                _LOGGER.warning("wecom_long_connection_replaced_by_new_owner")
            return
        if not self._authenticated or frame.get("cmd") != "aibot_msg_callback":
            return
        message = _text_message(frame)
        if message is None:
            return
        _LOGGER.info("wecom_long_connection_text_callback_received")
        reply = await self._on_text(message)
        if reply:
            await self._send_reply(message["request_id"], message["message_id"], reply)

    def _is_subscription_ack(self, frame: dict[str, Any], request_id: str) -> bool:
        headers = frame.get("headers")
        if not isinstance(headers, dict) or headers.get("req_id") != request_id:
            return False
        errcode = frame.get("errcode")
        if errcode is None and isinstance(frame.get("body"), dict):
            errcode = frame["body"].get("errcode")
        return errcode == 0

    def _start_heartbeat(self) -> None:
        if self._heartbeat_seconds <= 0 or self._heartbeat_task is not None:
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat())

    async def _heartbeat(self) -> None:
        while not self._closed and self._authenticated:
            await self._sleep(self._heartbeat_seconds)
            if not self._closed and self._authenticated:
                await self._send({"cmd": "ping", "headers": {"req_id": self._request_id()}})

    async def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _send_reply(self, request_id: str, message_id: str, content: str) -> None:
        await self._send(
            {
                "cmd": "aibot_respond_msg",
                "headers": {"req_id": request_id},
                "body": {
                    "msgtype": "stream",
                    "stream": {
                        "id": message_id,
                        "content": _reply_within_limit(content),
                        "finish": True,
                    },
                },
            }
        )

    async def _send(self, frame: dict[str, object]) -> None:
        if self._socket is None:
            raise RuntimeError("WECOM_WEBSOCKET_NOT_CONNECTED")
        await self._socket.send(json.dumps(frame, ensure_ascii=False, separators=(",", ":")))


def _text_message(frame: dict[str, Any]) -> dict[str, str] | None:
    headers = frame.get("headers")
    body = frame.get("body")
    if not isinstance(headers, dict) or not isinstance(body, dict):
        return None
    sender = body.get("from")
    text = body.get("text")
    if not isinstance(sender, dict) or not isinstance(text, dict) or body.get("msgtype") != "text":
        return None
    message = {
        "request_id": str(headers.get("req_id", "")),
        "message_id": str(body.get("msgid", "")),
        "bot_id": str(body.get("aibotid", "")),
        "chat_type": str(body.get("chattype", "")),
        "chat_id": str(body.get("chatid", "")),
        "from_user_id": str(sender.get("userid", "")),
        "text": str(text.get("content", "")),
        "response_url": str(body.get("response_url", "")),
    }
    if all(message[key] for key in ("request_id", "message_id", "bot_id", "from_user_id", "text")):
        return message
    return None


def _reply_within_limit(content: str) -> str:
    """Limit by UTF-8 bytes without splitting a multibyte character."""

    return content.encode("utf-8")[:MAX_WECOM_REPLY_BYTES].decode("utf-8", errors="ignore")
