"""Unit tests for the audit hash chain, signed manifests and the WORM boundary."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from trpc_service.audit_chain.chain import (
    GENESIS_HASH,
    canonical_event_json,
    chain_hash,
    event_hash,
    fingerprint,
)
from trpc_service.audit_chain.manifest import (
    sign_manifest,
    verify_manifest_signature,
)
from trpc_service.audit_chain.worm import MemoryWormArchive, S3ObjectLockWormArchive


def _event(occurred_at: str = "2026-09-05T00:00:00+00:00") -> dict[str, str]:
    return fingerprint(
        event_id="0198abcd-0000-7000-8000-000000000001",
        occurred_at=occurred_at,
        actor="admin-1",
        auth_method="oidc",
        action="tool.register",
        decision="ALLOW",
        target_type="tool",
        target_id="crm:v1",
        details={"side_effect": "READ_ONLY"},
    )


def test_fingerprint_hash_is_canonical_and_key_order_free() -> None:
    event = _event()
    shuffled = dict(reversed(list(event.items())))
    assert event_hash(event) == event_hash(shuffled)
    assert canonical_event_json(event) == canonical_event_json(shuffled)


def test_chain_hash_links_each_event_to_its_predecessor() -> None:
    first = chain_hash(GENESIS_HASH, event_hash(_event()))
    second = chain_hash(first, event_hash(_event("2026-09-05T00:00:01+00:00")))
    assert first != second
    assert chain_hash(GENESIS_HASH, event_hash(_event())) == first
    tampered = dict(_event())
    tampered["actor"] = "attacker"
    assert chain_hash(first, event_hash(tampered)) != second


def test_manifest_signature_detects_document_tampering() -> None:
    document = {
        "tenant_id": "t-1",
        "manifest_index": 1,
        "first_chain_index": 1,
        "last_chain_index": 3,
        "event_count": 3,
        "chain_head": "a" * 64,
        "created_at": "2026-09-05T00:00:00+00:00",
    }
    signature = sign_manifest(document, "audit-signing-key")
    assert verify_manifest_signature(document, signature, "audit-signing-key")
    tampered = {**document, "event_count": 4}
    assert not verify_manifest_signature(tampered, signature, "audit-signing-key")
    assert not verify_manifest_signature(document, signature, "other-key")


@pytest.mark.anyio
async def test_memory_worm_keeps_every_object_with_retention() -> None:
    archive = MemoryWormArchive()
    retain = datetime.now(UTC) + timedelta(days=365)
    location = await archive.put("bucket", "t-1/manifest.json", b"{}", retain_until=retain)
    assert location == "s3://bucket/t-1/manifest.json"
    stored, stored_retention = archive.objects["bucket/t-1/manifest.json"]
    assert stored == b"{}" and stored_retention == retain
    assert archive.writes == [("t-1/manifest.json", 2)]


@pytest.mark.anyio
async def test_s3_worm_write_uses_object_lock_compliance_headers() -> None:
    captured: dict[str, httpx.Headers] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = request.headers
        captured["content"] = request.content
        return httpx.Response(200)

    archive = S3ObjectLockWormArchive(
        endpoint_url="https://worm.internal:9000",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI",
    )
    real_async_client = httpx.AsyncClient

    def fake_client(**kwargs: Any) -> Any:  # type: ignore[name-defined]
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_async_client(**kwargs)

    import trpc_service.audit_chain.worm as worm_module

    original = worm_module.httpx.AsyncClient
    worm_module.httpx.AsyncClient = fake_client  # type: ignore[misc]
    try:
        retain = datetime.now(UTC) + timedelta(days=7)
        location = await archive.put(
            "audit-bucket", "t-1/m1.json", b"manifest", retain_until=retain
        )
    finally:
        worm_module.httpx.AsyncClient = original  # type: ignore[misc]
    assert location == "s3://audit-bucket/t-1/m1.json"
    headers = captured["headers"]
    assert headers["x-amz-object-lock-mode"] == "COMPLIANCE"
    assert headers["x-amz-content-sha256"] == hashlib.sha256(b"manifest").hexdigest()
    assert "AWS4-HMAC-SHA256" in headers["authorization"]
    assert "x-amz-object-lock-retain-until-date" in headers["authorization"]
