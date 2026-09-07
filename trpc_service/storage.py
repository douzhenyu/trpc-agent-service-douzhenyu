"""Runtime Storage Adapter contracts and tenant-isolated profile routing.

Adapters are process-local worker libraries, never network services.  Their
contract gives all supported backends identical tenant erasure semantics while
leaving immutable Session Events in an authoritative SQL/object backend.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.admin_api.database import Database
from trpc_service.governance import DataClassification


class AdapterKind(StrEnum):
    SQL = "SQL"
    REDIS = "REDIS"
    VECTOR = "VECTOR"
    OBJECT = "OBJECT"
    EXTERNAL_MEMORY = "EXTERNAL_MEMORY"


class StorageProfileError(RuntimeError):
    """Stable routing failure; callers must fail closed rather than widen scope."""


class StorageBackend(BaseModel):
    """A credential-free backend declaration held in a Storage Profile."""

    model_config = ConfigDict(frozen=True)

    kind: AdapterKind
    endpoint: str = Field(min_length=1, max_length=512)
    dedicated: bool
    secret_ref: str = Field(min_length=1, max_length=384)

    @field_validator("endpoint")
    @classmethod
    def endpoint_is_a_credential_free_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"https", "postgresql", "redis", "s3"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("STORAGE_ENDPOINT_INVALID")
        return value

    @field_validator("secret_ref")
    @classmethod
    def secret_is_a_vault_reference(cls, value: str) -> str:
        if not value.startswith("vault://") or "#" not in value:
            raise ValueError("STORAGE_SECRET_REF_INVALID")
        return value


class StorageProfile(BaseModel):
    """The complete runtime storage and compute isolation choice for one tenant."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str = Field(min_length=1, max_length=64)
    alias: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    classification: DataClassification
    worker_pool: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")
    encryption_key_ref: str = Field(min_length=1, max_length=384)
    backends: tuple[StorageBackend, ...]

    @model_validator(mode="after")
    def has_each_backend_once(self) -> StorageProfile:
        kinds = {backend.kind for backend in self.backends}
        required = {
            AdapterKind.SQL,
            AdapterKind.REDIS,
            AdapterKind.VECTOR,
            AdapterKind.OBJECT,
        }
        if not required.issubset(kinds) or len(kinds) != len(self.backends):
            raise ValueError("STORAGE_BACKENDS_INCOMPLETE")
        return self


class StorageAdapter(Protocol):
    """Common data-plane contract used by SQL, Redis, vector and object stores."""

    authoritative: bool

    async def put(self, tenant_id: str, key: str, value: bytes) -> None: ...

    async def get(self, tenant_id: str, key: str) -> bytes | None: ...

    async def erase_tenant(self, tenant_id: str) -> int: ...

    async def verify_tenant_erased(self, tenant_id: str) -> bool: ...


class _NamespacedMemoryAdapter:
    """Deterministic test adapter for a tenant-prefixed backend namespace."""

    authoritative = True

    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], bytes] = {}

    async def put(self, tenant_id: str, key: str, value: bytes) -> None:
        self._objects[(tenant_id, key)] = value

    async def get(self, tenant_id: str, key: str) -> bytes | None:
        return self._objects.get((tenant_id, key))

    async def erase_tenant(self, tenant_id: str) -> int:
        keys = [key for key in self._objects if key[0] == tenant_id]
        for key in keys:
            del self._objects[key]
        return len(keys)

    async def verify_tenant_erased(self, tenant_id: str) -> bool:
        return not any(stored_tenant == tenant_id for stored_tenant, _ in self._objects)


class InMemorySqlAdapter(_NamespacedMemoryAdapter):
    """Test double for a SQL authority (Session Events, audit, metadata)."""


class InMemoryVectorAdapter(_NamespacedMemoryAdapter):
    """Test double for a tenant-namespaced vector index."""


class InMemoryObjectAdapter(_NamespacedMemoryAdapter):
    """Test double for tenant-prefixed object storage."""


class ExternalMemoryAdapter(_NamespacedMemoryAdapter):
    """Adapter seam for an approved external Memory provider."""


class RedisCacheAdapter:
    """Rebuildable Redis cache backed by a separate authoritative adapter."""

    authoritative = False

    def __init__(self, authority: StorageAdapter) -> None:
        if not authority.authoritative:
            raise StorageProfileError("REDIS_AUTHORITY_REQUIRED")
        self._authority = authority
        self._cache: dict[tuple[str, str], bytes] = {}

    async def put(self, tenant_id: str, key: str, value: bytes) -> None:
        await self._authority.put(tenant_id, key, value)
        self._cache[(tenant_id, key)] = value

    async def get(self, tenant_id: str, key: str) -> bytes | None:
        cached = self._cache.get((tenant_id, key))
        if cached is not None:
            return cached
        value = await self._authority.get(tenant_id, key)
        if value is not None:
            self._cache[(tenant_id, key)] = value
        return value

    async def erase_tenant(self, tenant_id: str) -> int:
        cache_keys = [key for key in self._cache if key[0] == tenant_id]
        for key in cache_keys:
            del self._cache[key]
        authority_count = await self._authority.erase_tenant(tenant_id)
        return max(len(cache_keys), authority_count)

    async def clear_cache(self) -> None:
        self._cache.clear()

    async def verify_tenant_erased(self, tenant_id: str) -> bool:
        return not any(stored_tenant == tenant_id for stored_tenant, _ in self._cache) and (
            await self._authority.verify_tenant_erased(tenant_id)
        )


class StorageRouter:
    """Enforce storage and worker-pool isolation at the worker public boundary."""

    def __init__(self, profile: StorageProfile) -> None:
        self._profile = profile
        high_sensitivity = profile.classification in {
            DataClassification.CONFIDENTIAL,
            DataClassification.RESTRICTED,
        }
        if high_sensitivity and (
            profile.worker_pool == "shared-workers"
            or not all(b.dedicated for b in profile.backends)
        ):
            raise StorageProfileError("DEDICATED_STORAGE_REQUIRED")

    @property
    def worker_pool(self) -> str:
        return self._profile.worker_pool

    def namespace(self, kind: AdapterKind) -> str:
        if kind not in {backend.kind for backend in self._profile.backends}:
            raise StorageProfileError("STORAGE_BACKEND_UNAVAILABLE")
        suffix = "objects" if kind is AdapterKind.OBJECT else kind.value.lower()
        return f"{self._profile.tenant_id}/{suffix}"


class DatabaseStorageProfileResolver:
    """Resolve the active Storage Profile before a worker accepts work."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def resolve(self, tenant_id: UUID) -> StorageRouter | None:
        async with self._database.tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """SELECT tenant_id,alias,classification,worker_pool,encryption_key_ref,backends
                FROM tenant.storage_profile WHERE tenant_id=$1 AND active""",
                tenant_id,
            )
        if row is None:
            return None
        backends = row["backends"]
        if isinstance(backends, str):
            backends = json.loads(backends)
        return StorageRouter(
            StorageProfile(
                tenant_id=str(row["tenant_id"]),
                alias=str(row["alias"]),
                classification=DataClassification(str(row["classification"])),
                worker_pool=str(row["worker_pool"]),
                encryption_key_ref=str(row["encryption_key_ref"]),
                backends=tuple(StorageBackend.model_validate(value) for value in backends),
            )
        )


@dataclass(frozen=True)
class StorageDeletionReceipt:
    tenant_id: str
    deleted_by_backend: dict[AdapterKind, int]
    verified: bool


class StorageDeletionCoordinator:
    """Execute a tenant erasure across every configured Adapter with verification."""

    def __init__(self, adapters: dict[AdapterKind, StorageAdapter]) -> None:
        required = {
            AdapterKind.SQL,
            AdapterKind.REDIS,
            AdapterKind.VECTOR,
            AdapterKind.OBJECT,
        }
        if not required.issubset(adapters):
            raise StorageProfileError("STORAGE_DELETION_ADAPTERS_INCOMPLETE")
        self._adapters = adapters

    async def erase_tenant(
        self, tenant_id: str, *, legal_hold: bool = False
    ) -> StorageDeletionReceipt:
        if legal_hold:
            raise StorageProfileError("STORAGE_DELETION_LEGAL_HOLD")
        deleted = {
            kind: await adapter.erase_tenant(tenant_id) for kind, adapter in self._adapters.items()
        }
        verified = all(
            [await adapter.verify_tenant_erased(tenant_id) for adapter in self._adapters.values()]
        )
        if not verified:
            raise StorageProfileError("STORAGE_DELETION_UNVERIFIED")
        return StorageDeletionReceipt(tenant_id, deleted, verified)
