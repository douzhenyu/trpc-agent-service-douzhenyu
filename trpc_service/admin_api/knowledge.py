"""Tenant-scoped Knowledge Base, immutable Revision and Deployment Admin API."""

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
    KnowledgeBaseCreate,
    KnowledgeBaseResponse,
    KnowledgeDeploymentCreate,
    KnowledgeDeploymentResponse,
    KnowledgeDeploymentRollback,
    KnowledgeRevisionCreate,
    KnowledgeRevisionResponse,
)
from trpc_service.admin_api.tenant_access import require_tenant_access
from trpc_service.execution_bus import insert_outbox_record
from trpc_service.ids import uuid7
from trpc_service.knowledge import document_hash


def _revision_response(row: Any) -> dict[str, Any]:
    result = record_to_dict(row)
    result["version"] = result.pop("revision_version")
    return result


def _deployment_response(row: Any) -> dict[str, Any]:
    return record_to_dict(row)


def create_knowledge_router(database: Database) -> APIRouter:
    router = APIRouter(prefix="/api/v1/tenants/{tenant_id}/knowledge-bases", tags=["knowledge"])

    @router.post(
        "",
        response_model=KnowledgeBaseResponse,
        status_code=201,
        responses=error_responses(401, 403, 409, 422),
    )
    async def create_base(
        tenant_id: UUID,
        payload: KnowledgeBaseCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "knowledge_base.create",
            target_type="knowledge_base",
        )
        request_payload = {"tenant_id": str(tenant_id), **payload.model_dump(mode="json")}
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="knowledge_base.create",
                payload=request_payload,
            )
            if replayed is not None:
                return replayed
            row = await connection.fetchrow(
                """INSERT INTO tenant.knowledge_base (tenant_id,id,slug,name)
                VALUES ($1,$2,$3,$4) RETURNING tenant_id,id,slug,name,version,created_at""",
                tenant_id,
                uuid7(),
                payload.slug,
                payload.name,
            )
            if row is None:
                raise HTTPException(status_code=409, detail="Knowledge Base already exists")
            result = record_to_dict(row)
            await insert_audit(
                connection,
                principal,
                "knowledge_base.create",
                "ALLOW",
                target_type="knowledge_base",
                target_id=str(result["id"]),
                tenant_id=tenant_id,
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="knowledge_base.create",
                payload=request_payload,
                response=result,
            )
        return result

    @router.post(
        "/{base_id}/revisions",
        response_model=KnowledgeRevisionResponse,
        status_code=202,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def create_revision(
        tenant_id: UUID,
        base_id: UUID,
        payload: KnowledgeRevisionCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        await require_tenant_access(
            database,
            principal,
            tenant_id,
            "write",
            "knowledge_revision.create",
            target_type="knowledge_base",
            target_id=str(base_id),
        )
        if payload.chunking.overlap_chars >= payload.chunking.max_chars:
            raise HTTPException(
                status_code=422, detail="chunk overlap must be smaller than chunk size"
            )
        sources = [source.model_dump(mode="json") for source in payload.sources]
        source_snapshot = [
            {
                "source_ref": source["source_ref"],
                "content_hash": document_hash(str(source["content"])),
                "acl_subjects": sorted(source["acl_subjects"]),
                "data_classification": source["data_classification"],
            }
            for source in sources
        ]
        request_payload = {
            "tenant_id": str(tenant_id),
            "base_id": str(base_id),
            "sources": source_snapshot,
            "chunking": payload.chunking.model_dump(),
            "embedding_model": payload.embedding_model,
            "index_config": payload.index_config,
        }
        content_hash = hashlib.sha256(
            json.dumps(
                request_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        revision_id = uuid7()
        async with database.tenant_transaction(tenant_id) as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=key,
                operation="knowledge_revision.create",
                payload=request_payload,
            )
            if replayed is not None:
                return replayed
            await connection.fetchval(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", str(base_id)
            )
            base = await connection.fetchrow(
                "SELECT id FROM tenant.knowledge_base WHERE tenant_id=$1 AND id=$2",
                tenant_id,
                base_id,
            )
            if base is None:
                raise HTTPException(status_code=404, detail="Knowledge Base not found")
            version = await connection.fetchval(
                """SELECT coalesce(max(revision_version),0)+1 FROM tenant.knowledge_revision
                WHERE tenant_id=$1 AND base_id=$2""",
                tenant_id,
                base_id,
            )
            revision = await connection.fetchrow(
                """INSERT INTO tenant.knowledge_revision
                (tenant_id,id,base_id,revision_version,source_snapshot,chunking,embedding_model,index_config,content_hash)
                VALUES ($1,$2,$3,$4,CAST($5 AS jsonb),CAST($6 AS jsonb),$7,CAST($8 AS jsonb),$9)
                RETURNING tenant_id,id,base_id,revision_version,source_snapshot,chunking,
                embedding_model,index_config,content_hash,created_at""",
                tenant_id,
                revision_id,
                base_id,
                version,
                json.dumps(source_snapshot),
                json.dumps(payload.chunking.model_dump()),
                payload.embedding_model,
                json.dumps(payload.index_config),
                content_hash,
            )
            assert revision is not None
            await connection.execute(
                """INSERT INTO tenant.knowledge_revision_build (tenant_id,revision_id,status)
                VALUES ($1,$2,'BUILDING')""",
                tenant_id,
                revision_id,
            )
            for source in sources:
                document_id = uuid7()
                await connection.execute(
                    """INSERT INTO tenant.knowledge_document
                    (tenant_id,revision_id,id,source_ref,content,content_hash,data_classification)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                    tenant_id,
                    revision_id,
                    document_id,
                    source["source_ref"],
                    source["content"],
                    document_hash(str(source["content"])),
                    source["data_classification"],
                )
                await connection.executemany(
                    """INSERT INTO tenant.knowledge_document_acl
                    (tenant_id,revision_id,document_id,subject_id) VALUES ($1,$2,$3,$4)""",
                    [
                        (tenant_id, revision_id, document_id, subject)
                        for subject in source["acl_subjects"]
                    ],
                )
            await insert_outbox_record(
                connection,
                tenant_id=str(tenant_id),
                message_id=str(uuid7()),
                source="admin-api",
                event_type="knowledge.revision.build.requested",
                partition_key=f"{tenant_id}:{base_id}",
                payload_json=json.dumps(
                    {"tenant_id": str(tenant_id), "revision_id": str(revision_id)}
                ),
            )
            await insert_audit(
                connection,
                principal,
                "knowledge_revision.create",
                "ALLOW",
                target_type="knowledge_revision",
                target_id=str(revision_id),
                tenant_id=tenant_id,
                details={"base_id": str(base_id), "content_hash": content_hash},
            )
            result = _revision_response(
                {
                    **record_to_dict(revision),
                    "status": "BUILDING",
                    "validated_at": None,
                    "error_code": None,
                }
            )
            await remember(
                connection,
                actor=principal.subject,
                key=key,
                operation="knowledge_revision.create",
                payload=request_payload,
                response=result,
            )
        return result

    @router.post(
        "/{base_id}/deployments",
        response_model=KnowledgeDeploymentResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def deploy_revision(
        tenant_id: UUID,
        base_id: UUID,
        payload: KnowledgeDeploymentCreate,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        return await _create_deployment(
            database,
            tenant_id,
            base_id,
            payload.environment,
            payload.revision_id,
            payload.rollout_percentage,
            "DEPLOY",
            principal,
            key,
        )

    @router.post(
        "/{base_id}/deployments/rollback",
        response_model=KnowledgeDeploymentResponse,
        status_code=201,
        responses=error_responses(401, 403, 404, 409, 422),
    )
    async def rollback_deployment(
        tenant_id: UUID,
        base_id: UUID,
        payload: KnowledgeDeploymentRollback,
        principal: Annotated[Principal, Depends(principal_from_request)],
        key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> dict[str, Any]:
        return await _create_deployment(
            database,
            tenant_id,
            base_id,
            payload.environment,
            payload.revision_id,
            100,
            "ROLLBACK",
            principal,
            key,
        )

    return router


async def _create_deployment(
    database: Database,
    tenant_id: UUID,
    base_id: UUID,
    environment: str,
    revision_id: UUID,
    rollout_percentage: int,
    source_kind: str,
    principal: Principal,
    key: str,
) -> dict[str, Any]:
    await require_tenant_access(
        database,
        principal,
        tenant_id,
        "write",
        f"knowledge_deployment.{source_kind.lower()}",
        target_type="knowledge_base",
        target_id=str(base_id),
    )
    request_payload = {
        "tenant_id": str(tenant_id),
        "base_id": str(base_id),
        "environment": environment,
        "revision_id": str(revision_id),
        "rollout_percentage": rollout_percentage,
        "source_kind": source_kind,
    }
    async with database.tenant_transaction(tenant_id) as connection:
        replayed = await replay_for(
            connection,
            actor=principal.subject,
            key=key,
            operation="knowledge_deployment.create",
            payload=request_payload,
        )
        if replayed is not None:
            return replayed
        await connection.fetchval(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", str(base_id)
        )
        ready = await connection.fetchrow(
            """SELECT revision.id FROM tenant.knowledge_revision revision
            JOIN tenant.knowledge_revision_build build
              ON build.tenant_id=revision.tenant_id AND build.revision_id=revision.id
            WHERE revision.tenant_id=$1 AND revision.base_id=$2
              AND revision.id=$3 AND build.status='READY'""",
            tenant_id,
            base_id,
            revision_id,
        )
        if ready is None:
            raise HTTPException(status_code=409, detail="Knowledge Revision is not ready")
        previous = await connection.fetchrow(
            """SELECT revision_id FROM tenant.knowledge_deployment
            WHERE tenant_id=$1 AND base_id=$2 AND environment=$3
            ORDER BY created_at DESC,id DESC LIMIT 1""",
            tenant_id,
            base_id,
            environment,
        )
        row = await connection.fetchrow(
            """INSERT INTO tenant.knowledge_deployment
            (tenant_id,id,base_id,environment,revision_id,previous_revision_id,rollout_percentage,source_kind,created_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING id,tenant_id,base_id,environment,revision_id,previous_revision_id,
            rollout_percentage,source_kind,created_by,created_at""",
            tenant_id,
            uuid7(),
            base_id,
            environment,
            revision_id,
            previous["revision_id"] if previous is not None else None,
            rollout_percentage,
            source_kind,
            principal.subject,
        )
        assert row is not None
        result = _deployment_response(row)
        await insert_audit(
            connection,
            principal,
            "knowledge_deployment.create",
            "ALLOW",
            target_type="knowledge_deployment",
            target_id=str(result["id"]),
            tenant_id=tenant_id,
            details={
                "base_id": str(base_id),
                "revision_id": str(revision_id),
                "source_kind": source_kind,
            },
        )
        await remember(
            connection,
            actor=principal.subject,
            key=key,
            operation="knowledge_deployment.create",
            payload=request_payload,
            response=result,
        )
    return result
