"""HTTP conditional-write primitives for versioned Admin API resources."""

from __future__ import annotations

import re

_ETAG = re.compile(r'^"([1-9][0-9]*)"$')


def parse_if_match(value: str) -> int:
    match = _ETAG.fullmatch(value)
    if match is None:
        raise ValueError("If-Match must contain one quoted positive version")
    return int(match.group(1))
