"""Immutable Knowledge Revision build and retrieval boundaries."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from trpc_service.admin_api.database import Database


class KnowledgeRevisionError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class RetrievalResult:
    document_id: str
    source_ref: str
    content: str
    data_classification: str


def document_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


def split_content(content: str, *, max_chars: int, overlap_chars: int) -> list[str]:
    if max_chars < 1 or overlap_chars < 0 or overlap_chars >= max_chars:
        raise KnowledgeRevisionError("INVALID_CHUNKING")
    chunks: list[str] = []
    start = 0
    while start < len(content):
        end = min(start + max_chars, len(content))
        if end < len(content):
            boundary = content.rfind(" ", start, end)
            if boundary > start:
                end = boundary
        value = content[start:end].strip()
        if value:
            chunks.append(value)
        if end == len(content):
            break
        start = max(end - overlap_chars, start + 1)
    return chunks


class KnowledgeRevisionBuilder:
    """Idempotent Job Worker boundary for one independently built Revision."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def build(self, tenant_id: str, revision_id: str) -> None:
        try:
            tenant, revision = UUID(tenant_id), UUID(revision_id)
        except ValueError as error:
            raise KnowledgeRevisionError("REVISION_NOT_FOUND") from error
        failure: KnowledgeRevisionError | None = None
        async with self._database.tenant_transaction(tenant) as connection:
            row = await connection.fetchrow(
                """SELECT revision.chunking, build.status FROM tenant.knowledge_revision revision
                JOIN tenant.knowledge_revision_build build
                  ON build.tenant_id=revision.tenant_id AND build.revision_id=revision.id
                WHERE revision.tenant_id=$1 AND revision.id=$2 FOR UPDATE OF build""",
                tenant,
                revision,
            )
            if row is None:
                raise KnowledgeRevisionError("REVISION_NOT_FOUND")
            if row["status"] == "READY":
                return
            if row["status"] != "BUILDING":
                raise KnowledgeRevisionError("REVISION_BUILD_FAILED")
            chunking = dict(row["chunking"])
            documents = await connection.fetch(
                """SELECT id,content,content_hash FROM tenant.knowledge_document
                WHERE tenant_id=$1 AND revision_id=$2 ORDER BY id""",
                tenant,
                revision,
            )
            try:
                for document in documents:
                    content = str(document["content"])
                    if document_hash(content) != str(document["content_hash"]):
                        raise KnowledgeRevisionError("SOURCE_HASH_MISMATCH")
                    chunks = split_content(
                        content,
                        max_chars=int(chunking["max_chars"]),
                        overlap_chars=int(chunking["overlap_chars"]),
                    )
                    if not chunks:
                        raise KnowledgeRevisionError("EMPTY_DOCUMENT")
                    await connection.executemany(
                        """INSERT INTO tenant.knowledge_chunk
                        (tenant_id,revision_id,document_id,chunk_index,content)
                        VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""",
                        [
                            (tenant, revision, document["id"], index, chunk)
                            for index, chunk in enumerate(chunks)
                        ],
                    )
            except KnowledgeRevisionError as error:
                await connection.execute(
                    """UPDATE tenant.knowledge_revision_build SET status='FAILED',error_code=$3
                    WHERE tenant_id=$1 AND revision_id=$2""",
                    tenant,
                    revision,
                    error.code,
                )
                failure = error
            else:
                await connection.execute(
                    """UPDATE tenant.knowledge_revision_build
                    SET status='READY',validated_at=now() WHERE tenant_id=$1 AND revision_id=$2""",
                    tenant,
                    revision,
                )
        if failure is not None:
            raise failure


class DatabaseKnowledgeRetriever:
    """Storage-level tenant, Base, Revision and document-ACL retrieval enforcement."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def retrieve(
        self,
        *,
        tenant_id: str,
        base_id: str,
        revision_id: str,
        subject_id: str,
        query: str,
        limit: int = 8,
    ) -> list[RetrievalResult]:
        if not query.strip() or not 1 <= limit <= 100:
            return []
        try:
            tenant, base, revision = UUID(tenant_id), UUID(base_id), UUID(revision_id)
        except ValueError:
            return []
        async with self._database.tenant_transaction(tenant) as connection:
            rows = await connection.fetch(
                """SELECT document.id AS document_id,document.source_ref,chunk.content,
                          document.data_classification
                FROM tenant.knowledge_revision revision
                JOIN tenant.knowledge_revision_build build
                  ON build.tenant_id=revision.tenant_id AND build.revision_id=revision.id
                JOIN tenant.knowledge_chunk chunk
                  ON chunk.tenant_id=revision.tenant_id AND chunk.revision_id=revision.id
                JOIN tenant.knowledge_document document
                  ON document.tenant_id=chunk.tenant_id AND document.revision_id=chunk.revision_id
                 AND document.id=chunk.document_id
                JOIN tenant.knowledge_document_acl acl
                  ON acl.tenant_id=document.tenant_id AND acl.revision_id=document.revision_id
                 AND acl.document_id=document.id AND acl.subject_id=$4
                WHERE revision.tenant_id=$1 AND revision.base_id=$2 AND revision.id=$3
                  AND build.status='READY'
                  AND chunk.search_vector @@ websearch_to_tsquery('simple',$5)
                ORDER BY ts_rank(chunk.search_vector,websearch_to_tsquery('simple',$5)) DESC,
                         chunk.document_id,chunk.chunk_index LIMIT $6""",
                tenant,
                base,
                revision,
                subject_id,
                query,
                limit,
            )
        return [
            RetrievalResult(
                document_id=str(row["document_id"]),
                source_ref=str(row["source_ref"]),
                content=str(row["content"]),
                data_classification=str(row["data_classification"]),
            )
            for row in rows
        ]


class DatabaseKnowledgeDeploymentResolver:
    """Resolve a stable blue/green Knowledge Deployment once per execution."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def resolve(
        self, tenant_id: str, base_id: str, environment: str, session_id: str
    ) -> str | None:
        try:
            tenant, base = UUID(tenant_id), UUID(base_id)
        except ValueError:
            return None
        async with self._database.tenant_transaction(tenant) as connection:
            row = await connection.fetchrow(
                """SELECT revision_id,previous_revision_id,rollout_percentage
                FROM tenant.knowledge_deployment
                WHERE tenant_id=$1 AND base_id=$2 AND environment=$3
                ORDER BY created_at DESC,id DESC LIMIT 1""",
                tenant,
                base,
                environment,
            )
        if row is None:
            return None
        previous = row["previous_revision_id"]
        bucket = int.from_bytes(hashlib.sha256(session_id.encode()).digest()[:8], "big") % 100
        if (
            previous is not None
            and int(row["rollout_percentage"]) < 100
            and bucket >= int(row["rollout_percentage"])
        ):
            return str(previous)
        return str(row["revision_id"])
