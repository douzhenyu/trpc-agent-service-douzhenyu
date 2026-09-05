"""Signed Policy Bundle lifecycle: versioning, activation, canary and rollback.

Structured governance rules are compiled and HMAC-signed into immutable bundle
versions. Activation moves the stable pointer (retiring the previous active
version); a canary version receives a deterministic share of decisions by
Session bucket and can be rolled back by re-activating any older version.
Every resolved bundle is signature-verified before use; verification failures
and absent bundles fail closed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from trpc_service.admin_api.database import Database
from trpc_service.governance import (
    GovernanceRules,
    canary_bucket,
    compile_bundle,
    sign_bundle,
    verify_bundle,
)


class PolicyBundleError(RuntimeError):
    """Safe, stable error for callers; never embeds bundle internals."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ResolvedPolicy:
    """The signature-verified rules governing one decision."""

    version: int
    rules: GovernanceRules
    canary: bool


_BUNDLE_COLUMNS = """tenant_id,version,rules,bundle,signature,status,canary_percentage,
created_by,created_at,activated_at"""


class PolicyBundleService:
    """Version, sign, activate, canary and roll back governance policies."""

    def __init__(self, database: Database, *, signing_key: str) -> None:
        if not signing_key:
            raise ValueError("policy signing key is required")
        self._database = database
        self._signing_key = signing_key

    async def create_version(
        self, tenant_id: str, rules: GovernanceRules, *, actor: str
    ) -> dict[str, Any]:
        version = await self._next_version(tenant_id)
        signature = sign_bundle(rules, version, self._signing_key)
        bundle = compile_bundle(rules, version)
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                f"""INSERT INTO tenant.policy_bundle
                (tenant_id,version,rules,bundle,signature,status,canary_percentage,created_by)
                VALUES ($1,$2,CAST($3 AS jsonb),CAST($4 AS jsonb),$5,'DRAFT',0,$6)
                RETURNING {_BUNDLE_COLUMNS}""",
                UUID(tenant_id),
                version,
                json.dumps(rules.model_dump(mode="json")),
                json.dumps(bundle),
                signature,
                actor,
            )
        assert row is not None
        return dict(row)

    async def activate(
        self, tenant_id: str, version: int, *, canary_percentage: int = 0
    ) -> dict[str, Any]:
        """Activate a version (canary when a percentage is set); roll back older ones."""

        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            row = await connection.fetchrow(
                f"""SELECT {_BUNDLE_COLUMNS} FROM tenant.policy_bundle
                WHERE tenant_id=$1 AND version=$2 FOR UPDATE""",
                UUID(tenant_id),
                version,
            )
            if row is None:
                raise PolicyBundleError("POLICY_BUNDLE_NOT_FOUND")
            rules = GovernanceRules.model_validate(row["rules"])
            if not verify_bundle(rules, version, str(row["signature"]), self._signing_key):
                raise PolicyBundleError("POLICY_BUNDLE_SIGNATURE_INVALID")
            status = "CANARY" if canary_percentage > 0 else "ACTIVE"
            if status == "CANARY":
                # A canary coexists with the stable ACTIVE version; only the
                # previous canary retires.
                await connection.execute(
                    """UPDATE tenant.policy_bundle
                    SET status='RETIRED',canary_percentage=0
                    WHERE tenant_id=$1 AND status='CANARY' AND version<>$2""",
                    UUID(tenant_id),
                    version,
                )
            else:
                await connection.execute(
                    """UPDATE tenant.policy_bundle
                    SET status='RETIRED',canary_percentage=0
                    WHERE tenant_id=$1 AND status IN ('ACTIVE','CANARY') AND version<>$2""",
                    UUID(tenant_id),
                    version,
                )
            updated = await connection.fetchrow(
                f"""UPDATE tenant.policy_bundle
                SET status=$3,canary_percentage=$4,activated_at=now()
                WHERE tenant_id=$1 AND version=$2
                RETURNING {_BUNDLE_COLUMNS}""",
                UUID(tenant_id),
                version,
                status,
                canary_percentage,
            )
            assert updated is not None
            return dict(updated)

    async def resolve(self, tenant_id: str, decision_key: str) -> ResolvedPolicy | None:
        """Resolve the governing rules for one decision.

        Returns None only when the tenant has no bundles at all — callers
        treat that as allow-under-platform-defaults (the LLM Gateway backstop
        and OPA still gate egress). A canary version receives the
        deterministic Session-bucket share of decisions; every candidate is
        signature-verified before use, tampered bundles and inconsistent
        activation states fail closed.
        """

        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                f"""SELECT {_BUNDLE_COLUMNS} FROM tenant.policy_bundle
                WHERE tenant_id=$1 AND status IN ('ACTIVE','CANARY')
                ORDER BY version""",
                UUID(tenant_id),
            )
        if not rows:
            return None
        stable = next((row for row in rows if row["status"] == "ACTIVE"), None)
        canary = next((row for row in rows if row["status"] == "CANARY"), None)
        if canary is not None and stable is None:
            # A canary without a stable version cannot govern consistently:
            # non-bucket decisions would run ungoverned, so fail closed.
            raise PolicyBundleError("POLICY_BUNDLE_UNAVAILABLE")
        chosen = stable
        canary_used = False
        if (
            canary is not None
            and int(canary["canary_percentage"]) > 0
            and canary_bucket(decision_key) < int(canary["canary_percentage"])
        ):
            chosen = canary
            canary_used = True
        if chosen is None:
            return None
        rules = GovernanceRules.model_validate(chosen["rules"])
        version = int(chosen["version"])
        if not verify_bundle(rules, version, str(chosen["signature"]), self._signing_key):
            raise PolicyBundleError("POLICY_BUNDLE_SIGNATURE_INVALID")
        return ResolvedPolicy(version=version, rules=rules, canary=canary_used)

    async def list_versions(self, tenant_id: str) -> list[dict[str, Any]]:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            rows = await connection.fetch(
                f"""SELECT {_BUNDLE_COLUMNS} FROM tenant.policy_bundle
                WHERE tenant_id=$1 ORDER BY version DESC""",
                UUID(tenant_id),
            )
        return [dict(row) for row in rows]

    async def _next_version(self, tenant_id: str) -> int:
        async with self._database.tenant_transaction(UUID(tenant_id)) as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(hashtext($1))", tenant_id)
            version = await connection.fetchval(
                "SELECT coalesce(max(version),0)+1 FROM tenant.policy_bundle WHERE tenant_id=$1",
                UUID(tenant_id),
            )
        return int(version)


class PolicyBundleRulesResolver:
    """Adapts the bundle service to the Runner runtime rules resolver protocol."""

    def __init__(self, service: PolicyBundleService) -> None:
        self._service = service

    async def resolve_rules(self, tenant_id: str, decision_key: str) -> GovernanceRules | None:
        resolved = await self._service.resolve(tenant_id, decision_key)
        return resolved.rules if resolved is not None else None
