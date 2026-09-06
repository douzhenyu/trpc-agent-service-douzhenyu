"""Channel bindings: one external IM bot bound to one Agent application.

A binding carries only a 密钥引用 (secret reference) — an opaque handle the
Channel Gateway resolves at verification time; the secret value itself never
enters control-plane metadata. The (tenant, channel type, external bot id)
triple is unique, so one inbound route resolves to exactly one tenant and
one Agent application.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

_SECRET_REF_PATTERN = r"^vault://[a-z0-9/_:-]+#[a-zA-Z0-9_-]+$"
_CHANNEL_TYPES = ("FAKE", "WECOM", "FEISHU")


class SecretRefRejected(RuntimeError):
    """Raised when a value looks like a secret instead of a reference.

    RuntimeError so pydantic lets it propagate instead of wrapping it into
    a ValidationError that callers cannot catch specifically.
    """


class ChannelBindingConflict(RuntimeError):
    """Raised when an external bot is re-bound to a different application."""


class ChannelBindingStatus(StrEnum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class ChannelBinding(BaseModel):
    """The effective association between one external bot and one application."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    binding_id: str = Field(min_length=1, max_length=64)
    channel_type: str = Field(pattern=r"^(FAKE|WECOM|FEISHU)$")
    external_bot_id: str = Field(min_length=1, max_length=128)
    application_id: str = Field(min_length=1, max_length=64)
    environment: str = Field(pattern=r"^(DEVELOPMENT|STAGING|PRODUCTION)$")
    secret_ref: str = Field(min_length=1, max_length=512)
    status: ChannelBindingStatus = ChannelBindingStatus.ACTIVE

    def model_post_init(self, _context: object) -> None:
        """Only 密钥引用 shapes are storable; raw secrets fail construction."""

        if not re.match(_SECRET_REF_PATTERN, self.secret_ref):
            raise SecretRefRejected(
                "secret_ref must be an opaque vault reference, never a secret value"
            )

    @property
    def route_key(self) -> tuple[str, str, str]:
        return (self.tenant_id, self.channel_type, self.external_bot_id)


class BindingStore(Protocol):
    async def insert(self, binding: ChannelBinding, *, created_by: str = "") -> ChannelBinding: ...
    async def resolve(
        self, tenant_id: str, channel_type: str, external_bot_id: str
    ) -> ChannelBinding | None: ...


class MemoryBindingStore:
    def __init__(self) -> None:
        self._bindings: dict[tuple[str, str, str], ChannelBinding] = {}

    async def insert(self, binding: ChannelBinding, *, created_by: str = "") -> ChannelBinding:
        existing = self._bindings.get(binding.route_key)
        if existing is not None:
            if existing != binding:
                raise SecretRefRejected("channel binding conflict: external bot already bound")
            return existing
        self._bindings[binding.route_key] = binding
        return binding

    async def resolve(
        self, tenant_id: str, channel_type: str, external_bot_id: str
    ) -> ChannelBinding | None:
        return self._bindings.get((tenant_id, channel_type, external_bot_id))


class ChannelBindingRegistry:
    """Register and resolve tenant-scoped channel bindings."""

    def __init__(self, store: BindingStore) -> None:
        self._store = store

    @classmethod
    def in_memory(cls) -> ChannelBindingRegistry:
        return cls(MemoryBindingStore())

    async def register(self, binding: ChannelBinding, *, created_by: str = "") -> ChannelBinding:
        if binding.channel_type not in _CHANNEL_TYPES:
            raise ValueError(f"unsupported channel type: {binding.channel_type}")
        return await self._store.insert(binding, created_by=created_by)

    async def resolve(
        self, *, tenant_id: str, channel_type: str, external_bot_id: str
    ) -> ChannelBinding | None:
        binding = await self._store.resolve(tenant_id, channel_type, external_bot_id)
        if binding is None or binding.status != ChannelBindingStatus.ACTIVE:
            return None
        return binding
