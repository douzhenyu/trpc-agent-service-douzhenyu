"""Public contract tests for tenant runtime Storage Adapters."""

from __future__ import annotations

import asyncio

import pytest

from trpc_service.governance import DataClassification
from trpc_service.storage import (
    AdapterKind,
    ExternalMemoryAdapter,
    InMemoryObjectAdapter,
    InMemorySqlAdapter,
    InMemoryVectorAdapter,
    RedisCacheAdapter,
    StorageBackend,
    StorageDeletionCoordinator,
    StorageProfile,
    StorageProfileError,
    StorageRouter,
)


def test_sql_redis_vector_and_object_adapters_share_tenant_erasure_contract() -> None:
    async def exercise() -> None:
        authority = InMemorySqlAdapter()
        adapters = (
            authority,
            RedisCacheAdapter(authority),
            InMemoryVectorAdapter(),
            InMemoryObjectAdapter(),
        )
        for adapter in adapters:
            await adapter.put("tenant-a", "record", b"tenant-a-value")
            await adapter.put("tenant-b", "record", b"tenant-b-value")
            assert await adapter.get("tenant-a", "record") == b"tenant-a-value"
            assert await adapter.erase_tenant("tenant-a") == 1
            assert await adapter.get("tenant-a", "record") is None
            assert await adapter.get("tenant-b", "record") == b"tenant-b-value"

    asyncio.run(exercise())


def test_redis_is_not_the_only_authority_and_can_be_rebuilt() -> None:
    async def exercise() -> None:
        authority = InMemorySqlAdapter()
        cache = RedisCacheAdapter(authority)
        await cache.put("tenant-a", "session-1", b"authoritative event")
        await cache.clear_cache()
        assert await cache.get("tenant-a", "session-1") == b"authoritative event"
        assert cache.authoritative is False

    asyncio.run(exercise())


def test_cross_backend_erasure_is_verified_and_honors_legal_hold() -> None:
    async def exercise() -> None:
        adapters = {
            AdapterKind.SQL: InMemorySqlAdapter(),
            AdapterKind.VECTOR: InMemoryVectorAdapter(),
            AdapterKind.OBJECT: InMemoryObjectAdapter(),
            AdapterKind.REDIS: RedisCacheAdapter(InMemorySqlAdapter()),
            AdapterKind.EXTERNAL_MEMORY: ExternalMemoryAdapter(),
        }
        for adapter in adapters.values():
            await adapter.put("tenant-a", "record", b"erase me")
        coordinator = StorageDeletionCoordinator(adapters)
        receipt = await coordinator.erase_tenant("tenant-a")
        assert receipt.verified is True
        assert receipt.deleted_by_backend == {
            AdapterKind.SQL: 1,
            AdapterKind.VECTOR: 1,
            AdapterKind.OBJECT: 1,
            AdapterKind.REDIS: 1,
            AdapterKind.EXTERNAL_MEMORY: 1,
        }
        with pytest.raises(StorageProfileError, match="STORAGE_DELETION_LEGAL_HOLD"):
            await coordinator.erase_tenant("tenant-a", legal_hold=True)

    asyncio.run(exercise())


def test_restricted_tenant_requires_dedicated_backends_and_worker_pool() -> None:
    shared = StorageProfile(
        tenant_id="tenant-a",
        alias="shared",
        classification=DataClassification.RESTRICTED,
        worker_pool="shared-workers",
        encryption_key_ref="vault://tenant/tenant-a/storage#encryption_key",
        backends=tuple(
            StorageBackend(
                kind=kind,
                endpoint="https://shared.example.test",
                dedicated=False,
                secret_ref="vault://tenant/tenant-a/storage#credential",
            )
            for kind in AdapterKind
        ),
    )
    with pytest.raises(StorageProfileError, match="DEDICATED_STORAGE_REQUIRED"):
        StorageRouter(shared)

    dedicated = shared.model_copy(
        update={
            "worker_pool": "tenant-a-restricted-workers",
            "backends": tuple(
                StorageBackend(
                    kind=kind,
                    endpoint="https://tenant-a.example.test",
                    dedicated=True,
                    secret_ref="vault://tenant/tenant-a/storage#credential",
                )
                for kind in AdapterKind
            ),
        }
    )
    router = StorageRouter(dedicated)
    assert router.worker_pool == "tenant-a-restricted-workers"
    assert router.namespace(AdapterKind.OBJECT) == "tenant-a/objects"


def test_default_profile_uses_four_builtin_backends_and_external_memory_is_optional() -> None:
    profile = StorageProfile(
        tenant_id="tenant-a",
        alias="builtin-only",
        classification=DataClassification.INTERNAL,
        worker_pool="shared-workers",
        encryption_key_ref="vault://tenant/tenant-a/storage#encryption_key",
        backends=tuple(
            StorageBackend(
                kind=kind,
                endpoint=f"https://{kind.value.lower()}.example.test",
                dedicated=False,
                secret_ref=f"vault://tenant/tenant-a/storage#{kind.value.lower()}",
            )
            for kind in (AdapterKind.SQL, AdapterKind.REDIS, AdapterKind.VECTOR, AdapterKind.OBJECT)
        ),
    )
    assert StorageRouter(profile).namespace(AdapterKind.SQL) == "tenant-a/sql"
