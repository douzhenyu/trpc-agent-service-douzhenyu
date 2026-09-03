"""OIDC and emergency authentication at the Admin API boundary."""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
from dataclasses import dataclass
from typing import Any, Literal, cast
from urllib.parse import urlencode
from uuid import UUID

import httpx
import jwt
from fastapi import HTTPException, Request, status
from jwt.algorithms import RSAAlgorithm
from pwdlib import PasswordHash

from trpc_service.admin_api.settings import AdminSettings


@dataclass(frozen=True)
class Principal:
    subject: str
    auth_method: Literal["oidc", "emergency"]
    roles: frozenset[str]


def _secret(settings: AdminSettings) -> str:
    return settings.session_signing_key.get_secret_value()


def encode_session(settings: AdminSettings, principal: Principal) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "sub": principal.subject,
            "auth_method": principal.auth_method,
            "roles": sorted(principal.roles),
            "iat": now,
            "exp": now + settings.session_ttl_seconds,
            "type": "session",
        },
        _secret(settings),
        algorithm="HS256",
    )


def decode_session(settings: AdminSettings, token: str) -> Principal:
    try:
        claims = jwt.decode(token, _secret(settings), algorithms=["HS256"])
        if claims.get("type") != "session":
            raise jwt.InvalidTokenError("incorrect token type")
        auth_method = claims.get("auth_method")
        if auth_method not in ("oidc", "emergency"):
            raise jwt.InvalidTokenError("incorrect authentication method")
        return Principal(
            subject=str(claims["sub"]),
            auth_method=auth_method,
            roles=frozenset(str(role) for role in claims.get("roles", [])),
        )
    except (jwt.InvalidTokenError, KeyError) as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid session"
        ) from error


async def principal_from_request(request: Request) -> Principal:
    settings: AdminSettings = request.app.state.settings
    authorization = request.headers.get("authorization", "")
    token = authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else None
    token = token or request.cookies.get(settings.session_cookie_name)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required"
        )
    principal = decode_session(settings, token)
    if principal.auth_method == "oidc":
        try:
            user_id = UUID(principal.subject)
        except ValueError as error:
            raise HTTPException(status_code=401, detail="invalid session") from error
        database = request.app.state.db
        async with database.transaction() as connection:
            exists = await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM platform.platform_user WHERE id=$1)", user_id
            )
            roles = await connection.fetch(
                "SELECT role FROM platform.platform_role_assignment WHERE user_id=$1", user_id
            )
        if not exists:
            raise HTTPException(status_code=401, detail="invalid session")
        principal = Principal(
            principal.subject,
            principal.auth_method,
            frozenset(row["role"] for row in roles),
        )
    return principal


def require_role(principal: Principal, *roles: str) -> None:
    if not principal.roles.intersection(roles):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="insufficient role")


def verify_emergency_password(settings: AdminSettings, username: str, password: str) -> bool:
    encoded = settings.emergency_admin_password_hash.get_secret_value()
    if not encoded or not secrets.compare_digest(username, settings.emergency_admin_username):
        return False
    try:
        return PasswordHash.recommended().verify(password, encoded)
    except Exception:
        return False


def begin_oidc_flow(settings: AdminSettings) -> tuple[str, str]:
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    now = int(time.time())
    flow = jwt.encode(
        {
            "state": state,
            "nonce": nonce,
            "verifier": verifier,
            "iat": now,
            "exp": now + 300,
            "type": "oidc_flow",
        },
        _secret(settings),
        algorithm="HS256",
    )
    query = urlencode(
        {
            "response_type": "code",
            "client_id": settings.oidc_client_id,
            "redirect_uri": settings.oidc_redirect_uri,
            "scope": "openid profile email",
            "state": state,
            "nonce": nonce,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{settings.oidc_authorization_endpoint}?{query}", flow


async def complete_oidc_flow(
    settings: AdminSettings,
    code: str,
    state: str,
    flow_token: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    try:
        flow = jwt.decode(flow_token, _secret(settings), algorithms=["HS256"])
        if flow.get("type") != "oidc_flow" or not secrets.compare_digest(str(flow["state"]), state):
            raise jwt.InvalidTokenError("state mismatch")
    except (jwt.InvalidTokenError, KeyError) as error:
        raise HTTPException(status_code=400, detail="invalid oidc flow") from error

    try:
        async with httpx.AsyncClient(transport=transport, timeout=10) as client:
            token_response = await client.post(
                settings.oidc_token_endpoint,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.oidc_redirect_uri,
                    "client_id": settings.oidc_client_id,
                    "client_secret": settings.oidc_client_secret.get_secret_value(),
                    "code_verifier": flow["verifier"],
                },
            )
            if token_response.status_code != 200:
                raise HTTPException(status_code=401, detail="oidc code exchange failed")
            token_payload = token_response.json()
            if not isinstance(token_payload, dict):
                raise HTTPException(status_code=502, detail="identity provider unavailable")
            id_token = token_payload.get("id_token")
            if not id_token:
                raise HTTPException(status_code=401, detail="oidc id token missing")
            jwks_response = await client.get(settings.oidc_jwks_uri)
            jwks_response.raise_for_status()
            jwks = jwks_response.json()
            if not isinstance(jwks, dict) or not isinstance(jwks.get("keys"), list):
                raise HTTPException(status_code=502, detail="identity provider unavailable")
            keys = jwks["keys"]
    except (httpx.HTTPError, ValueError) as error:
        raise HTTPException(status_code=502, detail="identity provider unavailable") from error

    try:
        header = jwt.get_unverified_header(id_token)
        if header.get("alg") != "RS256" or not header.get("kid"):
            raise HTTPException(status_code=401, detail="unsupported oidc signing key")
        jwk = next(
            (key for key in keys if isinstance(key, dict) and key.get("kid") == header["kid"]),
            None,
        )
        if jwk is None:
            raise HTTPException(status_code=401, detail="unknown oidc signing key")
        public_key = cast(Any, RSAAlgorithm.from_jwk(jwk))
        claims = jwt.decode(
            id_token,
            public_key,
            algorithms=["RS256"],
            audience=settings.oidc_client_id,
            issuer=settings.oidc_issuer,
            options={"require": ["exp", "iat", "sub"]},
        )
        audience = claims.get("aud")
        if (
            isinstance(audience, list)
            and len(audience) > 1
            and claims.get("azp") != settings.oidc_client_id
        ):
            raise jwt.InvalidTokenError("azp does not identify this client")
        if not secrets.compare_digest(str(claims.get("nonce", "")), str(flow["nonce"])):
            raise jwt.InvalidTokenError("nonce mismatch")
        return claims
    except jwt.InvalidTokenError as error:
        raise HTTPException(status_code=401, detail="invalid oidc id token") from error
    except (TypeError, ValueError, jwt.PyJWTError) as error:
        raise HTTPException(status_code=502, detail="identity provider unavailable") from error
