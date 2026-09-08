"""Tenant- and subject-scoped Artifact lifecycle plus attachment screening."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePath
from typing import Any, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.admin_api.audit import write_audit
from trpc_service.admin_api.database import Database
from trpc_service.governance import DataClassification, highest_classification, scan_messages

ARTIFACT_NOT_FOUND = "ARTIFACT_NOT_FOUND"
ARTIFACT_TENANT_FORBIDDEN = "ARTIFACT_TENANT_FORBIDDEN"
ARTIFACT_SUBJECT_FORBIDDEN = "ARTIFACT_SUBJECT_FORBIDDEN"
ARTIFACT_EXPIRED = "ARTIFACT_EXPIRED"
ARTIFACT_ACCESS_TOKEN_INVALID = "ARTIFACT_ACCESS_TOKEN_INVALID"
ATTACHMENT_FILENAME_INVALID = "ATTACHMENT_FILENAME_INVALID"
ATTACHMENT_MEDIA_TYPE_FORBIDDEN = "ATTACHMENT_MEDIA_TYPE_FORBIDDEN"
ATTACHMENT_EXECUTABLE_FORBIDDEN = "ATTACHMENT_EXECUTABLE_FORBIDDEN"
ATTACHMENT_MALWARE_DETECTED = "ATTACHMENT_MALWARE_DETECTED"
ATTACHMENT_SECRET_BLOCKED = "ATTACHMENT_SECRET_BLOCKED"
ATTACHMENT_TOO_LARGE = "ATTACHMENT_TOO_LARGE"

DEFAULT_ARTIFACT_TTL = timedelta(days=30)
DEFAULT_ACCESS_TTL = timedelta(minutes=15)
MAX_ATTACHMENT_BYTES = 8 * 1024 * 1024
_ALLOWED_MEDIA_TYPES = frozenset(
    {
        "application/json",
        "application/pdf",
        "image/jpeg",
        "image/png",
        "text/csv",
        "text/markdown",
        "text/plain",
    }
)
_EXECUTABLE_SUFFIXES = frozenset(
    {
        ".apk",
        ".app",
        ".bat",
        ".cmd",
        ".com",
        ".dll",
        ".dmg",
        ".exe",
        ".jar",
        ".js",
        ".msi",
        ".ps1",
        ".py",
        ".sh",
        ".vbs",
    }
)
_MALWARE_SIGNATURES = (b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE",)
_EXECUTABLE_MAGIC = (
    b"MZ",  # PE/COFF
    b"\x7fELF",
    b"#!",  # scripts are executable content even when renamed
    b"\xca\xfe\xba\xbe",  # Java class
    b"\x00asm",  # WebAssembly
    b"\xfe\xed\xfa\xce",  # Mach-O 32-bit (big endian)
    b"\xce\xfa\xed\xfe",  # Mach-O 32-bit (little endian)
    b"\xfe\xed\xfa\xcf",  # Mach-O 64-bit (big endian)
    b"\xcf\xfa\xed\xfe",  # Mach-O 64-bit (little endian)
)


class ArtifactError(RuntimeError):
    """Stable artifact lifecycle error that never contains artifact content."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ArtifactAccessError(ArtifactError):
    """A capability or ownership check rejected an artifact read."""


class AttachmentRejected(ArtifactError):
    """Attachment validation failed before bytes can enter platform storage."""


class Artifact(BaseModel):
    """Metadata for a named, digested file object; content remains in its store."""

    model_config = ConfigDict(frozen=True)

    artifact_id: str = Field(min_length=1, max_length=64)
    tenant_id: str = Field(min_length=1, max_length=64)
    subject_id: str = Field(min_length=1, max_length=256)
    execution_id: str = Field(min_length=1, max_length=128)
    filename: str = Field(min_length=1, max_length=255)
    media_type: str = Field(min_length=1, max_length=128)
    size_bytes: int = Field(ge=0, le=MAX_ATTACHMENT_BYTES)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    classification: DataClassification
    created_at: datetime
    expires_at: datetime


@dataclass(frozen=True)
class ArtifactAuditEvent:
    action: str
    tenant_id: str
    artifact_id: str
    subject_id: str
    occurred_at: datetime
    outcome: str = "ALLOW"


class ArtifactStore(Protocol):
    async def put(self, artifact: Artifact, content: bytes) -> None: ...

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[Artifact, bytes] | None: ...

    async def delete(self, tenant_id: str, artifact_id: str) -> None: ...

    async def list_expired(self, tenant_id: str, now: datetime) -> list[Artifact]: ...


class ArtifactAuditSink(Protocol):
    async def emit(self, event: ArtifactAuditEvent) -> None: ...


ArtifactRetentionResolver = Callable[[str], Awaitable[int]]


class MemoryArtifactStore:
    """In-memory store used by tests and explicit local development wiring."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[Artifact, bytes]] = {}

    async def put(self, artifact: Artifact, content: bytes) -> None:
        self.objects[artifact.artifact_id] = (artifact, content)

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[Artifact, bytes] | None:
        stored = self.objects.get(artifact_id)
        return stored if stored is not None and stored[0].tenant_id == tenant_id else None

    async def delete(self, tenant_id: str, artifact_id: str) -> None:
        stored = await self.get(tenant_id, artifact_id)
        if stored is None:
            return
        self.objects.pop(artifact_id, None)

    async def list_expired(self, tenant_id: str, now: datetime) -> list[Artifact]:
        return [
            artifact
            for artifact, _content in self.objects.values()
            if artifact.tenant_id == tenant_id and artifact.expires_at <= now
        ]


class DatabaseArtifactStore:
    """Durable tenant-scoped Artifact bytes and metadata for the gateway."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def put(self, artifact: Artifact, content: bytes) -> None:
        async with self._database.tenant_transaction(UUID(artifact.tenant_id)) as connection:
            await connection.execute(
                """INSERT INTO tenant.artifact
                (tenant_id,artifact_id,subject_id,execution_id,filename,media_type,content,
                 size_bytes,sha256,classification,created_at,expires_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                UUID(artifact.tenant_id),
                UUID(artifact.artifact_id),
                artifact.subject_id,
                artifact.execution_id,
                artifact.filename,
                artifact.media_type,
                content,
                artifact.size_bytes,
                artifact.sha256,
                str(artifact.classification),
                artifact.created_at,
                artifact.expires_at,
            )

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[Artifact, bytes] | None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM tenant.artifact WHERE tenant_id=$1 AND artifact_id=$2",
                UUID(tenant_id),
                UUID(artifact_id),
            )
        return _artifact_from_row(row) if row is not None else None

    async def delete(self, tenant_id: str, artifact_id: str) -> None:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            await connection.execute(
                "DELETE FROM tenant.artifact WHERE tenant_id=$1 AND artifact_id=$2",
                UUID(tenant_id),
                UUID(artifact_id),
            )

    async def list_expired(self, tenant_id: str, now: datetime) -> list[Artifact]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                "SELECT * FROM tenant.artifact WHERE tenant_id=$1 AND expires_at <= $2",
                UUID(tenant_id),
                now,
            )
        return [_artifact_from_row(row)[0] for row in rows]


class DatabaseArtifactAuditSink:
    """Append Artifact lifecycle events to the durable tenant audit chain."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def emit(self, event: ArtifactAuditEvent) -> None:
        await write_audit(
            self._database,
            None,
            event.action,
            event.outcome,
            tenant_id=UUID(event.tenant_id),
            target_type="artifact",
            target_id=event.artifact_id,
            details={"subject_id": event.subject_id},
        )


class ArtifactService:
    """Create and serve artifacts only through expiring subject-bound grants."""

    def __init__(
        self,
        *,
        store: ArtifactStore,
        access_key: bytes,
        audit_sink: ArtifactAuditSink | None = None,
        clock: Callable[[], datetime] | None = None,
        retention_days: ArtifactRetentionResolver | None = None,
    ) -> None:
        if len(access_key) < 16:
            raise ValueError("artifact access_key must contain at least 16 bytes")
        self._store = store
        self._access_key = access_key
        self._clock = clock or (lambda: datetime.now(UTC))
        self._audit_sink = audit_sink
        self._retention_days = retention_days
        self.audit_events: list[ArtifactAuditEvent] = []

    async def create(
        self,
        *,
        tenant_id: str,
        subject_id: str,
        execution_id: str,
        filename: str,
        media_type: str,
        content: bytes,
        declared_classification: DataClassification,
        expires_at: datetime | None = None,
    ) -> Artifact:
        """Screen an attachment before persisting it under its tenant owner."""

        classification = _validate_attachment(
            filename=filename,
            media_type=media_type,
            content=content,
            declared_classification=declared_classification,
        )
        now = self._now()
        policy_days = await self._retention_days(tenant_id) if self._retention_days else None
        expiry = expires_at or now + timedelta(days=policy_days or DEFAULT_ARTIFACT_TTL.days)
        if expiry <= now:
            raise AttachmentRejected(ARTIFACT_EXPIRED)
        artifact = Artifact(
            artifact_id=str(uuid4()),
            tenant_id=tenant_id,
            subject_id=subject_id,
            execution_id=execution_id,
            filename=filename,
            media_type=media_type,
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            classification=classification,
            created_at=now,
            expires_at=expiry,
        )
        await self._store.put(artifact, content)
        try:
            await self._audit("artifact.created", artifact, subject_id)
        except Exception:
            # Audit is a creation precondition.  A failure must not leave a
            # readable object that is absent from the tamper-evident chain.
            await self._store.delete(tenant_id, artifact.artifact_id)
            raise
        return artifact

    async def issue_access_token(self, *, tenant_id: str, artifact_id: str, subject_id: str) -> str:
        """Issue a short lived signed capability; raw object paths are never exposed."""

        artifact, _content = await self._owned(tenant_id, artifact_id, subject_id)
        expiry = min(artifact.expires_at, self._now() + DEFAULT_ACCESS_TTL)
        token = self._sign(
            {
                "artifact_id": artifact.artifact_id,
                "expires_at": int(expiry.timestamp()),
                "subject_id": subject_id,
                "tenant_id": tenant_id,
            }
        )
        await self._audit("artifact.access_issued", artifact, subject_id)
        return token

    async def download(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        subject_id: str,
        access_token: str,
    ) -> bytes:
        """Return content only after tenant, owner, expiry and signature checks."""

        try:
            artifact, content = await self._owned(tenant_id, artifact_id, subject_id)
            self._verify(access_token, artifact=artifact, subject_id=subject_id)
        except ArtifactAccessError:
            stored = await self._store.get(tenant_id, artifact_id)
            if stored is not None:
                await self._audit("artifact.access_denied", stored[0], subject_id, outcome="DENY")
            raise
        await self._audit("artifact.accessed", artifact, subject_id)
        return content

    async def purge(self, *, tenant_id: str) -> int:
        """Delete expired objects when a lifecycle worker invokes this service."""

        expired = await self._store.list_expired(tenant_id, self._now())
        for artifact in expired:
            await self._store.delete(tenant_id, artifact.artifact_id)
            await self._audit("artifact.expired", artifact, artifact.subject_id)
        return len(expired)

    async def _owned(
        self, tenant_id: str, artifact_id: str, subject_id: str
    ) -> tuple[Artifact, bytes]:
        stored = await self._store.get(tenant_id, artifact_id)
        if stored is None:
            raise ArtifactAccessError(ARTIFACT_NOT_FOUND)
        artifact, content = stored
        if artifact.tenant_id != tenant_id:
            raise ArtifactAccessError(ARTIFACT_TENANT_FORBIDDEN)
        if artifact.expires_at <= self._now():
            raise ArtifactAccessError(ARTIFACT_EXPIRED)
        if artifact.subject_id != subject_id:
            raise ArtifactAccessError(ARTIFACT_SUBJECT_FORBIDDEN)
        return artifact, content

    def _verify(self, token: str, *, artifact: Artifact, subject_id: str) -> None:
        try:
            encoded, supplied_signature = token.split(".", 1)
            expected_signature = _b64url(
                hmac.new(self._access_key, encoded.encode(), hashlib.sha256).digest()
            )
            payload = json.loads(_b64url_decode(encoded))
            valid = (
                hmac.compare_digest(supplied_signature, expected_signature)
                and payload.get("artifact_id") == artifact.artifact_id
                and payload.get("subject_id") == subject_id
                and payload.get("tenant_id") == artifact.tenant_id
                and isinstance(payload.get("expires_at"), int)
                and int(payload["expires_at"]) <= int(artifact.expires_at.timestamp())
                and int(payload["expires_at"]) >= int(self._now().timestamp())
            )
        except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
            valid = False
        if not valid:
            raise ArtifactAccessError(ARTIFACT_ACCESS_TOKEN_INVALID)

    def _sign(self, payload: dict[str, object]) -> str:
        encoded = _b64url(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        signature = _b64url(hmac.new(self._access_key, encoded.encode(), hashlib.sha256).digest())
        return f"{encoded}.{signature}"

    async def _audit(
        self, action: str, artifact: Artifact, subject_id: str, *, outcome: str = "ALLOW"
    ) -> None:
        event = ArtifactAuditEvent(
            action, artifact.tenant_id, artifact.artifact_id, subject_id, self._now(), outcome
        )
        self.audit_events.append(event)
        if self._audit_sink is not None:
            await self._audit_sink.emit(event)

    def _now(self) -> datetime:
        return self._clock().astimezone(UTC)


class ArtifactLifecycleWorker:
    """Delete expired tenant Artifacts from the durable store on a cadence."""

    def __init__(self, database: Database, *, access_key: bytes) -> None:
        self._database = database
        self._service = ArtifactService(
            store=DatabaseArtifactStore(database),
            access_key=access_key,
            audit_sink=DatabaseArtifactAuditSink(database),
            retention_days=TenantArtifactRetention(database).days_for,
        )

    async def run_once(self) -> int:
        # Platform tenant identity is intentionally read outside tenant RLS;
        # each deletion itself is still performed in a tenant transaction.
        async with self._database.transaction() as connection:
            tenant_ids = await connection.fetch("SELECT id FROM platform.tenant")
        deleted = 0
        for row in tenant_ids:
            tenant_id = UUID(str(row["id"]))
            async with self._database.tenant_transaction(tenant_id) as connection:
                held = bool(
                    await connection.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM tenant.legal_hold "
                        "WHERE tenant_id=$1 AND status='ACTIVE')",
                        tenant_id,
                    )
                )
            if held:
                continue
            deleted += await self._service.purge(tenant_id=str(row["id"]))
        return deleted


class TenantArtifactRetention:
    """Resolve the approved artifact lifecycle limit without widening tenant scope."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def days_for(self, tenant_id: str) -> int:
        tenant_uuid = UUID(tenant_id)
        async with self._database.tenant_transaction(tenant_uuid) as connection:
            days = await connection.fetchval(
                "SELECT artifact_days FROM tenant.content_retention_policy WHERE tenant_id=$1",
                tenant_uuid,
            )
        return int(days) if days is not None else DEFAULT_ARTIFACT_TTL.days


def _validate_attachment(
    *, filename: str, media_type: str, content: bytes, declared_classification: DataClassification
) -> DataClassification:
    path = PurePath(filename)
    if (
        not filename
        or path.name != filename
        or any(character in filename for character in ("\x00", "\n", "\r", "\\"))
    ):
        raise AttachmentRejected(ATTACHMENT_FILENAME_INVALID)
    if path.suffix.lower() in _EXECUTABLE_SUFFIXES:
        raise AttachmentRejected(ATTACHMENT_EXECUTABLE_FORBIDDEN)
    if content.startswith(_EXECUTABLE_MAGIC):
        raise AttachmentRejected(ATTACHMENT_EXECUTABLE_FORBIDDEN)
    if media_type not in _ALLOWED_MEDIA_TYPES:
        raise AttachmentRejected(ATTACHMENT_MEDIA_TYPE_FORBIDDEN)
    if len(content) > MAX_ATTACHMENT_BYTES:
        raise AttachmentRejected(ATTACHMENT_TOO_LARGE)
    if any(signature in content for signature in _MALWARE_SIGNATURES):
        raise AttachmentRejected(ATTACHMENT_MALWARE_DETECTED)
    text = content.decode("utf-8", "replace")
    scan = scan_messages([{"content": text}])
    if scan.blocked:
        raise AttachmentRejected(ATTACHMENT_SECRET_BLOCKED)
    return highest_classification(declared_classification, scan.detected_classification)


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _artifact_from_row(row: Any) -> tuple[Artifact, bytes]:
    value = row  # asyncpg Record deliberately has mapping-style access only.
    artifact = Artifact(
        artifact_id=str(value["artifact_id"]),
        tenant_id=str(value["tenant_id"]),
        subject_id=str(value["subject_id"]),
        execution_id=str(value["execution_id"]),
        filename=str(value["filename"]),
        media_type=str(value["media_type"]),
        size_bytes=int(value["size_bytes"]),
        sha256=str(value["sha256"]),
        classification=DataClassification(str(value["classification"])),
        created_at=value["created_at"],
        expires_at=value["expires_at"],
    )
    return artifact, bytes(value["content"])
