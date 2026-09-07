"""Tests for channel-capability reply planning."""

import asyncio

from trpc_service.channels.adaptive_reply import (
    ChannelReplyCapabilities,
    ReplyStrategy,
    plan_reply,
)
from trpc_service.channels.delivery import (
    ChannelTransportOutcome,
    MemoryDeliveryStore,
    ReplyDelivery,
    ReplyDeliveryService,
)


def test_direct_conversation_uses_merged_streams_but_group_is_final_first() -> None:
    direct = plan_reply(
        "abcdefghij",
        capabilities=ChannelReplyCapabilities(
            supports_updates=True, max_text_chars=6, stream_chunk_chars=4
        ),
        is_group=False,
    )
    group = plan_reply(
        "abcdefghij",
        capabilities=ChannelReplyCapabilities(
            supports_updates=True, max_text_chars=6, stream_chunk_chars=4
        ),
        is_group=True,
    )

    assert direct.strategy is ReplyStrategy.MERGED_STREAM
    assert direct.stream_chunks == ("abcd", "efgh", "ij")
    assert direct.final_messages == ("abcdef", "ghij")
    assert group.strategy is ReplyStrategy.FINAL_ONLY
    assert group.processing_notice is True
    assert group.stream_chunks == ()
    assert group.final_messages == ("abcdef", "ghij")


def test_reply_segments_without_breaking_unicode_or_exceeding_channel_limit() -> None:
    plan = plan_reply(
        "甲乙丙丁戊己庚辛",
        capabilities=ChannelReplyCapabilities(
            supports_updates=False, max_text_chars=3, stream_chunk_chars=1
        ),
        is_group=False,
    )

    assert plan.strategy is ReplyStrategy.FINAL_ONLY
    assert plan.final_messages == ("甲乙丙", "丁戊己", "庚辛")
    assert all(len(segment) <= 3 for segment in plan.final_messages)


def test_updatable_card_delivery_reuses_one_logical_delivery_id() -> None:
    class Transport:
        def __init__(self) -> None:
            self.contents: list[str] = []

        async def send(self, delivery: ReplyDelivery, attempt_no: int) -> ChannelTransportOutcome:
            del attempt_no
            self.contents.append(delivery.content)
            return ChannelTransportOutcome(delivered=True)

        def reconcile(self, delivery: ReplyDelivery, attempt_no: int) -> str:
            del delivery, attempt_no
            return "delivered"

    async def exercise() -> None:
        transport = Transport()
        service = ReplyDeliveryService(store=MemoryDeliveryStore(), transport=transport)
        created = await service.enqueue(
            tenant_id="tenant-a",
            binding_id="binding-a",
            execution_id="execution-a",
            external_conversation_id="open_id:alice",
            content="处理中",
        )
        await service.run(created.delivery_id, tenant_id="tenant-a")
        updated = await service.update(
            created.delivery_id, tenant_id="tenant-a", content="最终回复"
        )
        await service.run(updated.delivery_id, tenant_id="tenant-a")
        assert updated.delivery_id == created.delivery_id
        assert transport.contents == ["处理中", "最终回复"]

    asyncio.run(exercise())
