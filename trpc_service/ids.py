"""Application-generated identifiers."""

import secrets
import time
from uuid import UUID


def uuid7() -> UUID:
    """Return an RFC 9562 UUIDv7 using millisecond time and secure randomness."""
    timestamp_ms = time.time_ns() // 1_000_000
    value = timestamp_ms << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return UUID(int=value)
