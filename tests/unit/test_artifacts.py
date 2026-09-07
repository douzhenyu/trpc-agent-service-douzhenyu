"""Artifact lifecycle and attachment safety at the public service boundary."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from trpc_service.artifacts import (
    ARTIFACT_EXPIRED,
    ARTIFACT_SUBJECT_FORBIDDEN,
    ATTACHMENT_EXECUTABLE_FORBIDDEN,
    ATTACHMENT_MALWARE_DETECTED,
    ArtifactAccessError,
    ArtifactService,
    AttachmentRejected,
    MemoryArtifactStore,
)
from trpc_service.governance import DataClassification


def test_artifact_access_is_bound_to_tenant_subject_expiry_and_audited() -> None:
    now = datetime(2026, 9, 7, tzinfo=UTC)
    clock = [now]
    service = ArtifactService(
        store=MemoryArtifactStore(),
        access_key=b"unit-test-access-key",
        clock=lambda: clock[0],
    )

    async def exercise() -> None:
        artifact = await service.create(
            tenant_id="tenant-a",
            subject_id="im:WECOM:binding-a:alice",
            execution_id="execution-a",
            filename="report.txt",
            media_type="text/plain",
            content=b"approved report",
            declared_classification=DataClassification.INTERNAL,
            expires_at=now + timedelta(minutes=90),
        )
        token = await service.issue_access_token(
            tenant_id="tenant-a",
            artifact_id=artifact.artifact_id,
            subject_id="im:WECOM:binding-a:alice",
        )

        clock[0] = now + timedelta(minutes=1)
        assert (
            await service.download(
                tenant_id="tenant-a",
                artifact_id=artifact.artifact_id,
                subject_id="im:WECOM:binding-a:alice",
                access_token=token,
            )
            == b"approved report"
        )
        with pytest.raises(ArtifactAccessError, match=ARTIFACT_SUBJECT_FORBIDDEN):
            await service.download(
                tenant_id="tenant-a",
                artifact_id=artifact.artifact_id,
                subject_id="im:WECOM:binding-a:bob",
                access_token=token,
            )

        clock[0] = now + timedelta(minutes=91)
        with pytest.raises(ArtifactAccessError, match=ARTIFACT_EXPIRED):
            await service.download(
                tenant_id="tenant-a",
                artifact_id=artifact.artifact_id,
                subject_id="im:WECOM:binding-a:alice",
                access_token=token,
            )
        assert [event.action for event in service.audit_events] == [
            "artifact.created",
            "artifact.access_issued",
            "artifact.accessed",
            "artifact.access_denied",
            "artifact.access_denied",
        ]

    asyncio.run(exercise())


def test_attachment_rejects_executables_and_malware_and_only_raises_classification() -> None:
    service = ArtifactService(store=MemoryArtifactStore(), access_key=b"unit-test-access-key")

    async def exercise() -> None:
        with pytest.raises(AttachmentRejected, match=ATTACHMENT_EXECUTABLE_FORBIDDEN):
            await service.create(
                tenant_id="tenant-a",
                subject_id="im:FEISHU:binding-a:alice",
                execution_id="execution-a",
                filename="invoice.exe",
                media_type="application/octet-stream",
                content=b"MZ...",
                declared_classification=DataClassification.PUBLIC,
            )
        with pytest.raises(AttachmentRejected, match=ATTACHMENT_EXECUTABLE_FORBIDDEN):
            await service.create(
                tenant_id="tenant-a",
                subject_id="im:FEISHU:binding-a:alice",
                execution_id="execution-a",
                filename="invoice.txt",
                media_type="text/plain",
                content=b"\xcf\xfa\xed\xfe disguised Mach-O",
                declared_classification=DataClassification.PUBLIC,
            )
        with pytest.raises(AttachmentRejected, match=ATTACHMENT_EXECUTABLE_FORBIDDEN):
            await service.create(
                tenant_id="tenant-a",
                subject_id="im:FEISHU:binding-a:alice",
                execution_id="execution-a",
                filename="invoice.txt",
                media_type="text/plain",
                content=b"MZ disguised executable",
                declared_classification=DataClassification.PUBLIC,
            )
        with pytest.raises(AttachmentRejected, match=ATTACHMENT_MALWARE_DETECTED):
            await service.create(
                tenant_id="tenant-a",
                subject_id="im:FEISHU:binding-a:alice",
                execution_id="execution-a",
                filename="scan.txt",
                media_type="text/plain",
                content=(b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"),
                declared_classification=DataClassification.PUBLIC,
            )
        artifact = await service.create(
            tenant_id="tenant-a",
            subject_id="im:FEISHU:binding-a:alice",
            execution_id="execution-a",
            filename="contacts.txt",
            media_type="text/plain",
            content="身份证号 11010519491231002X".encode(),
            declared_classification=DataClassification.PUBLIC,
        )
        assert artifact.classification is DataClassification.CONFIDENTIAL

    asyncio.run(exercise())


def test_purge_removes_expired_bytes_and_records_lifecycle_audit() -> None:
    now = datetime(2026, 9, 7, tzinfo=UTC)
    clock = [now]
    store = MemoryArtifactStore()
    service = ArtifactService(
        store=store, access_key=b"unit-test-access-key", clock=lambda: clock[0]
    )

    async def exercise() -> None:
        artifact = await service.create(
            tenant_id="tenant-a",
            subject_id="im:WECOM:binding-a:alice",
            execution_id="execution-a",
            filename="expired.txt",
            media_type="text/plain",
            content=b"expired payload",
            declared_classification=DataClassification.INTERNAL,
            expires_at=now + timedelta(seconds=1),
        )
        clock[0] = now + timedelta(seconds=2)
        assert await service.purge(tenant_id="tenant-a") == 1
        assert artifact.artifact_id not in store.objects
        assert service.audit_events[-1].action == "artifact.expired"

    asyncio.run(exercise())


def test_artifact_download_endpoint_requires_the_subject_bound_capability() -> None:
    from typing import Any, cast

    from trpc_service.admin_api.database import Database
    from trpc_service.channel_gateway import ChannelGatewaySettings, create_app

    service = ArtifactService(store=MemoryArtifactStore(), access_key=b"unit-test-access-key")

    async def prepare() -> tuple[str, str]:
        artifact = await service.create(
            tenant_id="tenant-a",
            subject_id="im:WECOM:binding-a:alice",
            execution_id="execution-a",
            filename="report.txt",
            media_type="text/plain",
            content=b"approved report",
            declared_classification=DataClassification.INTERNAL,
        )
        token = await service.issue_access_token(
            tenant_id="tenant-a",
            artifact_id=artifact.artifact_id,
            subject_id="im:WECOM:binding-a:alice",
        )
        return artifact.artifact_id, token

    artifact_id, token = asyncio.run(prepare())

    class OpenDatabase:
        async def open(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class Runner:
        async def close(self) -> None:
            return None

    app = create_app(
        ChannelGatewaySettings(database_url="postgresql://example.test/platform"),
        database=cast(Database, OpenDatabase()),
        runner=cast(Any, Runner()),
        artifact_service=service,
    )
    with TestClient(app) as client:
        allowed = client.get(
            f"/internal/v1/artifacts/{artifact_id}",
            headers={
                "X-Artifact-Tenant": "tenant-a",
                "X-Artifact-Subject": "im:WECOM:binding-a:alice",
                "X-Artifact-Access-Token": token,
            },
        )
        forbidden = client.get(
            f"/internal/v1/artifacts/{artifact_id}",
            headers={
                "X-Artifact-Tenant": "tenant-a",
                "X-Artifact-Subject": "im:WECOM:binding-a:bob",
                "X-Artifact-Access-Token": token,
            },
        )
    assert allowed.status_code == 200
    assert allowed.content == b"approved report"
    assert forbidden.status_code == 403
    assert forbidden.text == ARTIFACT_SUBJECT_FORBIDDEN
