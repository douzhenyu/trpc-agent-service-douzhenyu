"""Inbound channel events: verify, deduplicate, isolate and route to execution.

An inbound event resolves to exactly one channel binding, is verified with
signing material resolved from the binding's 密钥引用 (never stored), and is
recorded in a persistent idempotency ledger keyed by (tenant, binding,
message key). A duplicate delivery with the same payload reuses the original
execution through the gateway's message-id idempotency; the same key with a
different payload is isolated as a conflict and never routed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from enum import StrEnum
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.agent_gateway import AgentExecutionAccepted
from trpc_service.channels.bindings import ChannelBinding, ChannelBindingRegistry

FAKE_SIGNATURE_PREFIX = "fake:"


def fake_channel_signature(
    material: str, message_key: str, text: str, external_user_id: str
) -> str:
    """The Fake Channel signature: HMAC over the canonical inbound payload.

    Channel adapters copy this pattern: sign the payload, never compare a
    static token, and verify with a constant-time comparison.
    """

    digest = hmac.new(
        material.encode("utf-8"),
        f"{message_key}\n{text}\n{external_user_id}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{FAKE_SIGNATURE_PREFIX}{digest}"


class InboundError(RuntimeError):
    """Stable inbound failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class InboundStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    CONFLICTED = "CONFLICTED"


class InboundMessage(BaseModel):
    """One immutable inbound user input; duplicates are the same message."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    binding_id: str
    message_key: str = Field(min_length=1, max_length=256)
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    external_user_id: str = Field(min_length=1, max_length=128)
    execution_id: str | None = None
    release_id: str | None = None
    status: InboundStatus = InboundStatus.ACCEPTED
    occurred_at: str = Field(min_length=1)

    @property
    def ledger_key(self) -> tuple[str, str, str]:
        return (self.tenant_id, self.binding_id, self.message_key)


class InboundConflict(BaseModel):
    """Evidence that one message key arrived with two different payloads."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    binding_id: str
    message_key: str
    recorded_hash: str
    received_hash: str
    detected_at: str


class SecretResolver(Protocol):
    """Resolves signing material from a 密钥引用 at verification time."""

    def resolve(self, secret_ref: str) -> str: ...


class InboundStore(Protocol):
    async def insert(self, message: InboundMessage) -> tuple[InboundMessage, bool]: ...
    async def record_conflict(self, conflict: InboundConflict) -> None: ...
    async def attach_execution(
        self,
        tenant_id: str,
        binding_id: str,
        message_key: str,
        execution_id: str,
        release_id: str,
    ) -> None: ...


class MemoryInboundStore:
    def __init__(self) -> None:
        self.messages: dict[tuple[str, str, str], InboundMessage] = {}
        self.conflicts: list[InboundConflict] = []

    async def insert(self, message: InboundMessage) -> tuple[InboundMessage, bool]:
        existing = self.messages.get(message.ledger_key)
        if existing is not None:
            return existing, False
        self.messages[message.ledger_key] = message
        return message, True

    async def record_conflict(self, conflict: InboundConflict) -> None:
        self.conflicts.append(conflict)

    async def attach_execution(
        self,
        tenant_id: str,
        binding_id: str,
        message_key: str,
        execution_id: str,
        release_id: str,
    ) -> None:
        key = (tenant_id, binding_id, message_key)
        existing = self.messages.get(key)
        if existing is not None:
            self.messages[key] = existing.model_copy(
                update={"execution_id": execution_id, "release_id": release_id}
            )


def inbound_payload_hash(text: str, external_user_id: str) -> str:
    canonical = json.dumps(
        {"text": text, "external_user_id": external_user_id},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def execution_message_id(tenant_id: str, binding_id: str, message_key: str) -> str:
    """Stable message id so redeliveries reuse the original execution."""

    derived = uuid5(NAMESPACE_URL, f"inbound:{tenant_id}|{binding_id}|{message_key}")
    return f"inbound-{derived}"


class ExecutionSubmitter(Protocol):
    async def submit(self, submission: Any) -> AgentExecutionAccepted: ...


class ChannelInboundService:
    """Verifies, deduplicates and routes inbound channel events."""

    def __init__(
        self,
        *,
        registry: ChannelBindingRegistry,
        secrets: SecretResolver,
        submitter: ExecutionSubmitter,
        store: InboundStore,
    ) -> None:
        self._registry = registry
        self._secrets = secrets
        self._submitter = submitter
        self._store = store

    def subject_for(self, *, binding: ChannelBinding, external_user_id: str) -> str:
        """The IM subject identity scoped to this tenant and binding."""

        return f"im:{binding.channel_type}:{binding.binding_id}:{external_user_id}"

    async def signed_event(
        self,
        *,
        tenant_id: str,
        channel_type: str,
        external_bot_id: str,
        message_key: str,
        text: str,
        external_user_id: str,
    ) -> dict[str, str]:
        """Build the internal ledger event with its integrity signature.

        Protocol adapters verify their own channel signatures first; this
        signs the normalized fields so the inbound ledger can detect
        same-key different-payload quarantines downstream.
        """

        binding = await self._registry.resolve(
            tenant_id=tenant_id,
            channel_type=channel_type,
            external_bot_id=external_bot_id,
        )
        if binding is None:
            raise InboundError("BINDING_NOT_FOUND")
        material = self._secrets.resolve(binding.secret_ref)
        return {
            "channel_type": channel_type,
            "external_bot_id": external_bot_id,
            "message_key": message_key,
            "text": text,
            "external_user_id": external_user_id,
            "signature": fake_channel_signature(material, message_key, text, external_user_id),
        }

    async def ingest(self, *, tenant_id: str, event: dict[str, str]) -> AgentExecutionAccepted:
        from datetime import UTC, datetime

        for field in ("channel_type", "external_bot_id", "message_key", "text", "external_user_id"):
            if not event.get(field):
                raise InboundError("EVENT_INVALID")
        binding = await self._registry.resolve(
            tenant_id=tenant_id,
            channel_type=event["channel_type"],
            external_bot_id=event["external_bot_id"],
        )
        if binding is None:
            raise InboundError("BINDING_NOT_FOUND")
        material = self._secrets.resolve(binding.secret_ref)
        expected = fake_channel_signature(
            material, event["message_key"], event["text"], event["external_user_id"]
        )
        if not hmac.compare_digest(event.get("signature", ""), expected):
            raise InboundError("SIGNATURE_INVALID")

        return await self.ingest_verified(tenant_id=tenant_id, event=event)

    async def ingest_verified(
        self, *, tenant_id: str, event: dict[str, str]
    ) -> AgentExecutionAccepted:
        """Persist an event after its Channel Adapter verified its native protocol."""

        from datetime import UTC, datetime

        for field in ("channel_type", "external_bot_id", "message_key", "text", "external_user_id"):
            if not event.get(field):
                raise InboundError("EVENT_INVALID")
        binding = await self._registry.resolve(
            tenant_id=tenant_id,
            channel_type=event["channel_type"],
            external_bot_id=event["external_bot_id"],
        )
        if binding is None:
            raise InboundError("BINDING_NOT_FOUND")

        payload_hash = inbound_payload_hash(event["text"], event["external_user_id"])
        message = InboundMessage(
            tenant_id=tenant_id,
            binding_id=binding.binding_id,
            message_key=event["message_key"],
            payload_hash=payload_hash,
            external_user_id=event["external_user_id"],
            occurred_at=datetime.now(UTC).isoformat(),
        )
        stored, created = await self._store.insert(message)
        if (
            not created
            and stored.payload_hash == payload_hash
            and stored.execution_id
            and stored.release_id
        ):
            # Ledger hit: reuse the original execution without re-submitting.
            return AgentExecutionAccepted(
                execution_id=UUID(stored.execution_id),
                release_id=UUID(stored.release_id),
                session_id=(
                    f"channel:{binding.binding_id}:"
                    f"{event.get('session_key', event['external_user_id'])}"
                ),
                deduplicated=True,
            )
        if not created and stored.payload_hash != payload_hash:
            # Same key, different payload: isolate the newcomer, keep the
            # original execution untouched.
            await self._store.record_conflict(
                InboundConflict(
                    tenant_id=tenant_id,
                    binding_id=binding.binding_id,
                    message_key=event["message_key"],
                    recorded_hash=stored.payload_hash,
                    received_hash=payload_hash,
                    detected_at=datetime.now(UTC).isoformat(),
                )
            )
            raise InboundError("INBOUND_PAYLOAD_CONFLICT")

        message_id = execution_message_id(tenant_id, binding.binding_id, event["message_key"])
        submission = self._build_submission(binding, event, message_id)
        accepted = await self._submitter.submit(submission)
        # Attach unconditionally: the update is idempotent and heals the
        # crash window between the gateway commit and the ledger write.
        await self._store.attach_execution(
            tenant_id,
            binding.binding_id,
            event["message_key"],
            str(accepted.execution_id),
            str(accepted.release_id),
        )
        return accepted

    def _build_submission(
        self, binding: ChannelBinding, event: dict[str, str], message_id: str
    ) -> Any:
        from uuid import UUID

        from trpc_service.agent_gateway import AgentExecutionSubmission

        session_scope = event.get("session_key", event["external_user_id"])
        session_id = f"channel:{binding.binding_id}:{session_scope}"
        return AgentExecutionSubmission(
            tenant_id=UUID(binding.tenant_id),
            application_id=UUID(binding.application_id),
            environment=binding.environment,
            session_id=session_id,
            messages=[{"role": "user", "content": event["text"]}],
            message_id=message_id,
        )
