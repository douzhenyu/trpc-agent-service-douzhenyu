"""Versioned Eval Suite management for Agent Release promotion gates."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal, principal_from_request
from trpc_service.admin_api.database import Database, record_to_dict
from trpc_service.admin_api.http_contract import error_responses
from trpc_service.admin_api.idempotency import remember, replay_for
from trpc_service.admin_api.schemas import (
    EvalCanaryObservationCreate,
    EvalCanaryObservationResponse,
    EvalRunCreate,
    EvalRunResponse,
    EvalSuiteCreate,
    EvalSuiteResponse,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.evals import evaluate_evidence
from trpc_service.ids import uuid7
from trpc_service.version import TRPC_AGENT_VERSION


def _suite_response(row: Any) -> dict[str, Any]:
    result = record_to_dict(row)
    result["version"] = result.pop("suite_version")
    return result


def _run_response(row: Any) -> dict[str, Any]:
    return record_to_dict(row)


async def _dependency_snapshot(
    connection: Any, *, tenant_id: UUID, release: Any, suite: Any
) -> dict[str, Any]:
    draft_snapshot = dict(release["draft_snapshot"])
    tool_aliases = [str(alias) for alias in draft_snapshot.get("tool_aliases", [])]
    tool_rows = await connection.fetch(
        """SELECT DISTINCT ON (name) name,version FROM tenant.tool_definition
        WHERE tenant_id=$1 AND name = ANY(CAST($2 AS text[])) ORDER BY name,version DESC""",
        tenant_id,
        tool_aliases,
    )
    tool_versions = {str(row["name"]): int(row["version"]) for row in tool_rows}
    policy_rows = await connection.fetch(
        """SELECT version,status FROM tenant.policy_bundle
        WHERE tenant_id=$1 AND status IN ('ACTIVE','CANARY') ORDER BY version""",
        tenant_id,
    )
    knowledge_refs = [str(reference) for reference in draft_snapshot.get("knowledge_refs", [])]
    revision_ids: list[UUID] = []
    for reference in knowledge_refs:
        try:
            revision_ids.append(UUID(reference))
        except ValueError:
            continue
    revision_rows = await connection.fetch(
        """SELECT id,revision_version,content_hash FROM tenant.knowledge_revision
        WHERE tenant_id=$1 AND id = ANY(CAST($2 AS uuid[]))""",
        tenant_id,
        revision_ids,
    )
    revisions = {str(row["id"]): row for row in revision_rows}
    return {
        "sdk_version": TRPC_AGENT_VERSION,
        "release_content_hash": str(release["content_hash"]),
        "suite_content_hash": str(suite["content_hash"]),
        "models": release["model_profiles"],
        "knowledge": knowledge_refs,
        "knowledge_versions": [
            {
                "reference": reference,
                "revision_version": int(revisions[reference]["revision_version"]),
                "content_hash": str(revisions[reference]["content_hash"]),
            }
            for reference in knowledge_refs
            if reference in revisions
        ],
        "tools": tool_aliases,
        "tool_versions": [
            {"name": alias, "version": tool_versions.get(alias)} for alias in tool_aliases
        ],
        "policy": draft_snapshot.get("governance_policy_ref"),
        "policy_versions": [
            {"version": int(row["version"]), "status": str(row["status"])} for row in policy_rows
        ],
    }


def create_eval_router(database: Database) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/tenants/{tenant_id}/agent-applications/{application_id}",
        tags=["evals"],
    )

    @router.post(
        "/eval-suites",
        response_model=EvalSuiteResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def create_eval_suite(
        tenant_id: UUID,
        application_id: UUID,
        payload: EvalSuiteCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "eval_suite.create",
            target_type="agent_application",
            target_id=str(application_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            **payload.model_dump(mode="json"),
        }
        content_hash = hashlib.sha256(
            json.dumps(request_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_suite.create",
                payload=request_payload,
            )
            if replayed is not None:
                return replayed
            application = await connection.fetchrow(
                "SELECT id FROM tenant.agent_application WHERE tenant_id=$1 AND id=$2 FOR UPDATE",
                tenant_id,
                application_id,
            )
            if application is None:
                raise HTTPException(status_code=404, detail="Agent application not found")
            version = await connection.fetchval(
                """SELECT coalesce(max(suite_version),0)+1 FROM tenant.eval_suite
                WHERE tenant_id=$1 AND application_id=$2 AND slug=$3""",
                tenant_id,
                application_id,
                payload.slug,
            )
            row = await connection.fetchrow(
                """INSERT INTO tenant.eval_suite
                (tenant_id,id,application_id,slug,suite_version,dataset,scorers,thresholds,
                deterministic_assertions,content_hash,created_by)
                VALUES ($1,$2,$3,$4,$5,CAST($6 AS jsonb),CAST($7 AS jsonb),CAST($8 AS jsonb),
                CAST($9 AS jsonb),$10,$11)
                RETURNING id,tenant_id,application_id,slug,suite_version,dataset,scorers,thresholds,
                deterministic_assertions,content_hash,created_at""",
                tenant_id,
                uuid7(),
                application_id,
                payload.slug,
                version,
                json.dumps(payload.dataset),
                json.dumps(payload.scorers),
                json.dumps(payload.thresholds),
                json.dumps(payload.deterministic_assertions),
                content_hash,
                principal.subject,
            )
            assert row is not None
            result = _suite_response(row)
            await insert_audit(
                connection,
                principal,
                "eval_suite.create",
                "ALLOW",
                target_type="eval_suite",
                target_id=str(result["id"]),
                tenant_id=tenant_id,
                details={"application_id": str(application_id), "content_hash": content_hash},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_suite.create",
                payload=request_payload,
                response=result,
            )
        return result

    @router.post(
        "/eval-runs",
        response_model=EvalRunResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def create_eval_run(
        tenant_id: UUID,
        application_id: UUID,
        payload: EvalRunCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "eval_run.create",
            target_type="agent_application",
            target_id=str(application_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            **payload.model_dump(mode="json"),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_run.create",
                payload=request_payload,
            )
            if replayed is not None:
                return replayed
            suite = await connection.fetchrow(
                """SELECT id,content_hash,thresholds,deterministic_assertions FROM tenant.eval_suite
                WHERE tenant_id=$1 AND id=$2 AND application_id=$3""",
                tenant_id,
                payload.suite_id,
                application_id,
            )
            if suite is None:
                raise HTTPException(status_code=404, detail="Eval Suite not found")
            release = await connection.fetchrow(
                """SELECT id,content_hash,model_profiles,draft_snapshot FROM tenant.agent_release
                WHERE tenant_id=$1 AND id=$2 AND application_id=$3""",
                tenant_id,
                payload.release_id,
                application_id,
            )
            if release is None:
                raise HTTPException(status_code=404, detail="Agent Release not found")
            snapshot = await _dependency_snapshot(
                connection, tenant_id=tenant_id, release=release, suite=suite
            )
            results = evaluate_evidence(
                thresholds=dict(suite["thresholds"]),
                deterministic_assertions=list(suite["deterministic_assertions"]),
                evidence=payload.evidence,
            )
            content_hash = hashlib.sha256(
                json.dumps(
                    {"request": request_payload, "snapshot": snapshot, "results": results},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            row = await connection.fetchrow(
                """INSERT INTO tenant.eval_run
                (tenant_id,id,application_id,suite_id,release_id,environment,sdk_version,
                dependency_snapshot,evidence,results,status,content_hash,created_by)
                VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS jsonb),CAST($9 AS jsonb),
                CAST($10 AS jsonb),$11,$12,$13)
                RETURNING id,tenant_id,application_id,suite_id,release_id,environment,sdk_version,
                dependency_snapshot,results,status,content_hash,created_at""",
                tenant_id,
                uuid7(),
                application_id,
                payload.suite_id,
                payload.release_id,
                payload.environment,
                TRPC_AGENT_VERSION,
                json.dumps(snapshot),
                json.dumps(payload.evidence),
                json.dumps(results),
                results["status"],
                content_hash,
                principal.subject,
            )
            assert row is not None
            result = _run_response(row)
            await insert_audit(
                connection,
                principal,
                "eval_run.create",
                "ALLOW",
                target_type="eval_run",
                target_id=str(result["id"]),
                tenant_id=tenant_id,
                details={"release_id": str(payload.release_id), "status": results["status"]},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_run.create",
                payload=request_payload,
                response=result,
            )
        return result

    @router.post(
        "/deployments/{deployment_id}/eval-canary-observations",
        response_model=EvalCanaryObservationResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def record_eval_canary_observation(
        tenant_id: UUID,
        application_id: UUID,
        deployment_id: UUID,
        payload: EvalCanaryObservationCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "eval_canary_observation.create",
            target_type="agent_deployment",
            target_id=str(deployment_id),
        )
        request_payload = {
            "tenant_id": str(tenant_id),
            "application_id": str(application_id),
            "deployment_id": str(deployment_id),
            **payload.model_dump(mode="json"),
        }
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_canary_observation.create",
                payload=request_payload,
            )
            if replayed is not None:
                return replayed
            deployment = await connection.fetchrow(
                """SELECT id,release_id,status FROM tenant.agent_deployment
                WHERE tenant_id=$1 AND id=$2 AND application_id=$3 AND environment='PRODUCTION'
                FOR UPDATE""",
                tenant_id,
                deployment_id,
                application_id,
            )
            if deployment is None:
                raise HTTPException(status_code=404, detail="Agent Deployment not found")
            if deployment["status"] != "ACTIVE":
                raise HTTPException(status_code=409, detail="Agent Deployment is not active")
            run = await connection.fetchrow(
                """SELECT run.id,suite.thresholds FROM tenant.eval_run AS run
                JOIN tenant.eval_suite AS suite
                  ON suite.tenant_id=run.tenant_id AND suite.id=run.suite_id
                WHERE run.tenant_id=$1 AND run.id=$2 AND run.application_id=$3
                AND run.release_id=$4 AND run.environment='PRODUCTION' AND run.status='PASSED'""",
                tenant_id,
                payload.eval_run_id,
                application_id,
                deployment["release_id"],
            )
            if run is None:
                raise HTTPException(status_code=409, detail="EVAL_RUN_REQUIRED")
            result = evaluate_evidence(
                thresholds=dict(run["thresholds"]),
                deterministic_assertions=[],
                evidence={"metrics": payload.metrics},
            )
            decision = "HALTED" if result["status"] == "FAILED" else "CONTINUE"
            if decision == "HALTED":
                await connection.execute(
                    """UPDATE tenant.agent_deployment
                    SET status='HALTED',halted_at=now(),
                    halt_reason='EVAL_CANARY_THRESHOLD_EXCEEDED',
                    version=version+1 WHERE tenant_id=$1 AND id=$2""",
                    tenant_id,
                    deployment_id,
                )
            row = await connection.fetchrow(
                """INSERT INTO tenant.eval_canary_observation
                (tenant_id,id,deployment_id,eval_run_id,metrics,decision,created_by)
                VALUES ($1,$2,$3,$4,CAST($5 AS jsonb),$6,$7)
                RETURNING id,tenant_id,deployment_id,eval_run_id,metrics,decision,created_at""",
                tenant_id,
                uuid7(),
                deployment_id,
                payload.eval_run_id,
                json.dumps(payload.metrics),
                decision,
                principal.subject,
            )
            assert row is not None
            response = record_to_dict(row)
            await insert_audit(
                connection,
                principal,
                "eval_canary_observation.create",
                "ALLOW",
                target_type="agent_deployment",
                target_id=str(deployment_id),
                tenant_id=tenant_id,
                details={"decision": decision, "eval_run_id": str(payload.eval_run_id)},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="eval_canary_observation.create",
                payload=request_payload,
                response=response,
            )
        return response

    return router
