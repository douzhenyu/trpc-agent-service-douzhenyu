"""Stable sandbox error codes."""

from __future__ import annotations


class SandboxError(RuntimeError):
    """Stable sandbox failure carrying an error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code
