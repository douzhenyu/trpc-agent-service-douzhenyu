"""Opaque UUIDv7 cursors shared by Admin API list endpoints."""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from uuid import UUID


def encode_cursor(identifier: UUID) -> str:
    return urlsafe_b64encode(identifier.bytes).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> UUID | None:
    if cursor is None:
        return None
    try:
        return UUID(bytes=urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    except (ValueError, TypeError) as error:
        raise ValueError("invalid cursor") from error
