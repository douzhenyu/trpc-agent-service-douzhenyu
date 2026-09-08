from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from trpc_service.agent.runner import RunnerExecutionCommand
from trpc_service.agent_gateway import AgentExecutionAccepted
from trpc_service.channel_gateway import ChannelGatewaySettings, create_app
from trpc_service.channels.bindings import ChannelBinding, ChannelBindingRegistry


class FakeInbound:
    def __init__(self) -> None:
        self.signed: list[dict[str, str]] = []
        self.ingest_calls = 0

    async def signed_event(self, **event: str) -> dict[str, str]:
        self.signed.append(event)
        return event

    async def ingest(self, *, tenant_id: str, event: dict[str, str]) -> AgentExecutionAccepted:
        del tenant_id, event
        self.ingest_calls += 1
        return AgentExecutionAccepted(
            execution_id=uuid4(),
            release_id=uuid4(),
            session_id="channel-session",
            deduplicated=self.ingest_calls > 1,
        )


class FakeRunner:
    def __init__(self) -> None:
        self.requests: list[RunnerExecutionCommand] = []
        self.closed = False

    async def complete(self, request: RunnerExecutionCommand) -> object:
        self.requests.append(request)
        return type("Reply", (), {"content": "agent reply"})()

    async def close(self) -> None:
        self.closed = True


class FakeLongConnection:
    def __init__(self, handler: object) -> None:
        self.handler = handler
        self.started = asyncio.Event()
        self.closed = asyncio.Event()

    async def run(self) -> None:
        self.started.set()
        await self.closed.wait()

    async def close(self) -> None:
        self.closed.set()


def test_long_connection_requires_a_tenant_and_bot_credentials_but_not_callback_token() -> None:
    settings = ChannelGatewaySettings(
        database_url="postgresql://unused",
        wecom_transport="long_connection",
        wecom_tenant_id="tenant-1",
        wecom_bot_id="bot-1",
        wecom_bot_secret="not-logged",
    )

    settings.validate_runtime()

    with pytest.raises(RuntimeError, match="WECOM_TENANT_ID"):
        ChannelGatewaySettings(
            database_url="postgresql://unused",
            wecom_transport="long_connection",
            wecom_bot_id="bot-1",
            wecom_bot_secret="not-logged",
        ).validate_runtime()


@pytest.mark.asyncio
async def test_gateway_long_connection_uses_existing_inbound_pipeline_and_stops_cleanly() -> None:
    inbound = FakeInbound()
    runner = FakeRunner()
    created: list[FakeLongConnection] = []

    def factory(handler: object) -> FakeLongConnection:
        connection = FakeLongConnection(handler)
        created.append(connection)
        return connection

    app = create_app(
        ChannelGatewaySettings(
            database_url="postgresql://unused",
            wecom_transport="long_connection",
            wecom_tenant_id="tenant-1",
            wecom_bot_id="bot-1",
            wecom_bot_secret="not-logged",
        ),
        database=object(),
        inbound=inbound,  # type: ignore[arg-type]
        deliveries=object(),  # type: ignore[arg-type]
        runner=runner,  # type: ignore[arg-type]
        wecom_long_connection_factory=factory,
    )

    async with app.router.lifespan_context(app):
        registry = ChannelBindingRegistry.in_memory()
        await registry.register(
            ChannelBinding(
                tenant_id="tenant-1",
                binding_id="binding-1",
                channel_type="WECOM",
                external_bot_id="bot-1",
                application_id="application-1",
                environment="DEVELOPMENT",
                secret_ref="vault://tenant/tenant-1/channels/wecom#callback",
            )
        )
        app.state.registry = registry
        connection = created[0]
        await connection.started.wait()
        handler = connection.handler
        first = await handler(  # type: ignore[operator]
            {
                "request_id": "request-1",
                "message_id": "message-1",
                "bot_id": "bot-1",
                "chat_type": "single",
                "chat_id": "user-1",
                "from_user_id": "user-1",
                "text": "hello",
                "response_url": "",
            }
        )
        duplicate = await handler(  # type: ignore[operator]
            {
                "request_id": "request-2",
                "message_id": "message-1",
                "bot_id": "bot-1",
                "chat_type": "single",
                "chat_id": "user-1",
                "from_user_id": "user-1",
                "text": "hello",
                "response_url": "",
            }
        )

        assert first == "agent reply"
        assert duplicate is None
        assert inbound.signed == [
            {
                "tenant_id": "tenant-1",
                "channel_type": "WECOM",
                "external_bot_id": "bot-1",
                "message_key": "message-1",
                "text": "hello",
                "external_user_id": "user-1",
            },
            {
                "tenant_id": "tenant-1",
                "channel_type": "WECOM",
                "external_bot_id": "bot-1",
                "message_key": "message-1",
                "text": "hello",
                "external_user_id": "user-1",
            },
        ]
        assert len(runner.requests) == 1
        command = runner.requests[0]
        assert command.tenant_id == "tenant-1"
        assert command.application_id == "application-1"
        assert command.message == "hello"

    assert connection.closed.is_set()
    assert runner.closed
