"""WORM archive boundary: signed manifests land in immutable object storage.

The S3-compatible implementation writes with Object Lock in COMPLIANCE mode,
so even the platform's own credentials cannot delete or overwrite archived
evidence before the retention date. The in-memory implementation exists for
tests and local development.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime
from typing import Protocol

import httpx


class WormArchive(Protocol):
    async def put(
        self,
        bucket: str,
        key: str,
        content: bytes,
        *,
        retain_until: datetime,
    ) -> str: ...


class MemoryWormArchive:
    """In-memory stand-in; keeps every version ever written."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.writes: list[tuple[str, int]] = []

    async def put(self, bucket: str, key: str, content: bytes, *, retain_until: datetime) -> str:
        self.objects[f"{bucket}/{key}"] = (content, retain_until)
        self.writes.append((key, len(content)))
        return f"s3://{bucket}/{key}"


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


class S3ObjectLockWormArchive:
    """S3-compatible WORM writer with AWS Signature V4 and Object Lock."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._endpoint = endpoint_url.rstrip("/")
        self._access_key = access_key_id
        self._secret_key = secret_access_key.encode("utf-8")
        self._region = region

    async def put(
        self,
        bucket: str,
        key: str,
        content: bytes,
        *,
        retain_until: datetime,
    ) -> str:
        host = httpx.URL(self._endpoint).host
        assert host is not None
        now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        datestamp = now[:8]
        payload_hash = hashlib.sha256(content).hexdigest()
        canonical_headers = (
            f"host:{host}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{now}\n"
            f"x-amz-object-lock-mode:COMPLIANCE\n"
            f"x-amz-object-lock-retain-until-date:{retain_until.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        )
        signed_headers = (
            "host;x-amz-content-sha256;x-amz-date;"
            "x-amz-object-lock-mode;x-amz-object-lock-retain-until-date"
        )
        canonical_request = (
            f"PUT\n/{bucket}/{key}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )
        scope = f"{datestamp}/{self._region}/s3/aws4_request"
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{now}\n{scope}\n"
            + hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        )
        signing_key = _sign(
            _sign(
                _sign(_sign(b"AWS4" + self._secret_key, datestamp), self._region),
                "s3",
            ),
            "aws4_request",
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        headers = {
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": now,
            "x-amz-object-lock-mode": "COMPLIANCE",
            "x-amz-object-lock-retain-until-date": retain_until.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={self._access_key}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(
                f"{self._endpoint}/{bucket}/{key}", content=content, headers=headers
            )
        if response.status_code not in (200, 201):
            raise RuntimeError(f"WORM archive write failed: {response.status_code}")
        return f"s3://{bucket}/{key}"
