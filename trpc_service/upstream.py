"""Upstream release discovery: stable-only candidates for the pinned SDK."""

from __future__ import annotations

import re

_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def parse_version(version: str) -> tuple[int, int, int] | None:
    """Parse X.Y.Z; anything with pre/post/dev/local markers is not parseable."""
    match = _SEMVER.match(version.strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def is_stable_version(version: str) -> bool:
    """A stable release is exactly X.Y.Z — no rc/beta/dev/post suffix."""
    return parse_version(version) is not None


def latest_stable(releases: list[str], current: str) -> str | None:
    """The newest stable release strictly greater than ``current``, or None.

    Pre-releases are never candidates, so the daily check can never propose
    an unstable upgrade to the platform.
    """
    current_parsed = parse_version(current)
    if current_parsed is None:
        return None
    candidates: list[tuple[int, int, int]] = []
    for release in releases:
        parsed = parse_version(release)
        if parsed is not None and parsed > current_parsed:
            candidates.append(parsed)
    if not candidates:
        return None
    best = max(candidates)
    return f"{best[0]}.{best[1]}.{best[2]}"


def assert_supported(current: str, installed: str) -> None:
    """Guard used by tests/tooling to keep runtime and lockfile aligned."""
    if current != installed:
        raise RuntimeError(f"upstream pin mismatch: lockfile={current} runtime={installed}")
