import pytest

from trpc_service.admin_api.settings import AdminSettings


def test_runtime_rejects_missing_or_public_session_signing_key() -> None:
    with pytest.raises(RuntimeError, match="SESSION_SIGNING_KEY"):
        AdminSettings(session_signing_key="").validate_runtime_security()
    with pytest.raises(RuntimeError, match="SESSION_SIGNING_KEY"):
        AdminSettings(session_signing_key="development-only-change-me").validate_runtime_security()


def test_runtime_accepts_a_strong_injected_session_signing_key() -> None:
    AdminSettings(
        session_signing_key="a-runtime-secret-that-is-at-least-32-chars",
        oidc_enabled=False,
    ).validate_runtime_security()
