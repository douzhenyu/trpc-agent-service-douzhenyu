"""Signed audit manifests: one verifiable statement per archived segment.

A manifest pins a tenant's chain segment (first/last index, event count,
chain head) and is signed with HMAC-SHA256. Recomputing the chain over the
stored events proves the segment is intact; the signature proves the
manifest itself was issued by the platform. Manifests are written to WORM
storage before the manifest row commits.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from trpc_service.admin_api.database import Database
from trpc_service.audit_chain.worm import WormArchive

DEFAULT_RETENTION_DAYS = 2555  # seven years, WORM COMPLIANCE mode


class AuditManifestError(RuntimeError):
    """Stable manifest failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def sign_manifest(document: dict[str, Any], signing_key: str) -> str:
    return hmac.new(
        signing_key.encode("utf-8"),
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_manifest_signature(document: dict[str, Any], signature: str, signing_key: str) -> bool:
    return hmac.compare_digest(sign_manifest(document, signing_key), signature)


class AuditManifestRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: str
    manifest_index: int
    first_chain_index: int
    last_chain_index: int
    event_count: int
    chain_head_hash: str
    signature: str
    worm_location: str
    created_at: datetime


class AuditManifestService:
    """Builds, verifies and archives signed audit manifests."""

    def __init__(
        self,
        database: Database,
        *,
        signing_key: str,
        worm: WormArchive,
        bucket: str = "trpc-audit-worm",
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ) -> None:
        self._database = database
        self._signing_key = signing_key
        self._worm = worm
        self._bucket = bucket
        self._retention_days = retention_days

    async def build_and_archive(self, tenant_id: str) -> AuditManifestRecord | None:
        """Seal every not-yet-archived chain segment of one tenant."""

        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            last_archived = await connection.fetchval(
                "SELECT COALESCE(MAX(last_chain_index),0) FROM platform.audit_manifest "
                "WHERE tenant_id=$1",
                UUID(tenant_id),
            )
            rows = await connection.fetch(
                """SELECT chain_index, event_hash, prev_event_hash
                FROM platform.audit_event
                WHERE tenant_id=$1 AND chain_index > $2
                ORDER BY chain_index""",
                UUID(tenant_id),
                int(last_archived or 0),
            )
            if not rows:
                return None
            first_index = int(rows[0]["chain_index"])
            last_index = int(rows[-1]["chain_index"])
            # Recompute the rolling chain across the segment for integrity:
            # start from the previous segment head (the first event's prev).
            chain_head = str(rows[0]["prev_event_hash"])
            for row in rows:
                chain_head = _chain(chain_head, str(row["event_hash"]))
            created_at = datetime.now(UTC)
            manifest_index = await connection.fetchval(
                "SELECT COALESCE(MAX(manifest_index),0) + 1 FROM platform.audit_manifest "
                "WHERE tenant_id=$1",
                UUID(tenant_id),
            )
            document = {
                "tenant_id": tenant_id,
                "manifest_index": int(manifest_index),
                "first_chain_index": first_index,
                "last_chain_index": last_index,
                "event_count": len(rows),
                "chain_head": chain_head,
                "created_at": created_at.isoformat(),
            }
            signature = sign_manifest(document, self._signing_key)
            key = f"{tenant_id}/manifest-{int(manifest_index):012d}.json"
            retain_until = datetime.now(UTC) + timedelta(days=self._retention_days)
            location = await self._worm.put(
                self._bucket,
                key,
                json.dumps({**document, "signature": signature}, indent=2).encode("utf-8"),
                retain_until=retain_until,
            )
            await connection.execute(
                """INSERT INTO platform.audit_manifest
                    (tenant_id,manifest_index,first_chain_index,last_chain_index,
                     event_count,chain_head_hash,signature,worm_location,created_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                UUID(tenant_id),
                int(manifest_index),
                first_index,
                last_index,
                len(rows),
                chain_head,
                signature,
                location,
                created_at,
            )
            return AuditManifestRecord(
                tenant_id=tenant_id,
                manifest_index=int(manifest_index),
                first_chain_index=first_index,
                last_chain_index=last_index,
                event_count=len(rows),
                chain_head_hash=chain_head,
                signature=signature,
                worm_location=location,
                created_at=created_at,
            )

    async def verify_tenant_evidence(self, tenant_id: str) -> dict[str, Any]:
        """Recompute the tenant chain and check every manifest signature."""

        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            from trpc_service.audit_chain.chain import verify_tenant_chain

            chain = await verify_tenant_chain(connection, tenant_id)
            manifest_rows = await connection.fetch(
                "SELECT * FROM platform.audit_manifest WHERE tenant_id=$1 ORDER BY manifest_index",
                UUID(tenant_id),
            )
        manifests_valid = True
        for row in manifest_rows:
            document = {
                "tenant_id": tenant_id,
                "manifest_index": int(row["manifest_index"]),
                "first_chain_index": int(row["first_chain_index"]),
                "last_chain_index": int(row["last_chain_index"]),
                "event_count": int(row["event_count"]),
                "chain_head": str(row["chain_head_hash"]),
                "created_at": row["created_at"].isoformat()
                if hasattr(row["created_at"], "isoformat")
                else str(row["created_at"]),
            }
            if not verify_manifest_signature(document, str(row["signature"]), self._signing_key):
                manifests_valid = False
                break
        return {
            "chain": chain,
            "manifests": len(manifest_rows),
            "manifest_signatures_valid": manifests_valid,
            "tamper_detected": not chain.get("valid", False) or not manifests_valid,
        }


def _chain(previous_hash: str, current_event_hash: str) -> str:
    from trpc_service.audit_chain.chain import chain_hash

    return chain_hash(previous_hash, current_event_hash)
