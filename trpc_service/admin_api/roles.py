"""Transactional platform-role assignment boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from trpc_service.admin_api.audit import insert_audit
from trpc_service.admin_api.auth import Principal
from trpc_service.admin_api.database import Database
from trpc_service.admin_api.idempotency import remember, replay_for

PlatformRole = Literal["PLATFORM_ADMIN", "PLATFORM_AUDITOR"]


class PlatformUserNotFoundError(Exception):
    """The role assignment target does not exist."""


class PlatformUserVersionChangedError(Exception):
    """The supplied version is stale."""


@dataclass(frozen=True)
class RoleAssignmentResult:
    version: int
    replayed: bool = False


async def assign_platform_role(
    database: Database,
    principal: Principal,
    *,
    user_id: UUID,
    role: PlatformRole,
    expected_version: int,
    idempotency_key: str,
) -> RoleAssignmentResult:
    request_payload = {
        "user_id": str(user_id),
        "role": role,
        "version": expected_version,
    }
    try:
        async with database.transaction() as connection:
            replayed = await replay_for(
                connection,
                actor=principal.subject,
                key=idempotency_key,
                operation="platform_role.assign",
                payload=request_payload,
            )
            if replayed is not None:
                return RoleAssignmentResult(version=int(replayed["version"]), replayed=True)

            current = await connection.fetchrow(
                """SELECT u.version,
                EXISTS(SELECT 1 FROM platform.platform_role_assignment r
                  WHERE r.user_id=u.id AND r.role=$2) assigned
                FROM platform.platform_user u WHERE u.id=$1""",
                user_id,
                role,
            )
            if current is None:
                raise PlatformUserNotFoundError
            if current["version"] != expected_version:
                raise PlatformUserVersionChangedError
            if current["assigned"]:
                await remember(
                    connection,
                    actor=principal.subject,
                    key=idempotency_key,
                    operation="platform_role.assign",
                    payload=request_payload,
                    response={"version": expected_version},
                )
                return RoleAssignmentResult(version=expected_version)

            updated = await connection.fetchval(
                """UPDATE platform.platform_user SET version=version+1,updated_at=now()
                WHERE id=$1 AND version=$2 RETURNING version""",
                user_id,
                expected_version,
            )
            if updated is None:
                raise PlatformUserVersionChangedError
            await connection.execute(
                """INSERT INTO platform.platform_role_assignment (user_id,role)
                VALUES ($1,$2)""",
                user_id,
                role,
            )
            await insert_audit(
                connection,
                principal,
                "platform_role.assign",
                "ALLOW",
                target_type="platform_user",
                target_id=str(user_id),
                details={"role": role},
            )
            await remember(
                connection,
                actor=principal.subject,
                key=idempotency_key,
                operation="platform_role.assign",
                payload=request_payload,
                response={"version": updated},
            )
            return RoleAssignmentResult(version=int(updated))
    except IntegrityError as error:
        raise PlatformUserNotFoundError from error
