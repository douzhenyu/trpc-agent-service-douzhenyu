"""Configuration for the public Admin API."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AdminSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = "postgresql://trpc_platform_app@localhost/trpc_platform"
    session_signing_key: SecretStr = SecretStr("")
    session_cookie_name: str = "trpc_platform_session"
    session_cookie_secure: bool = True
    policy_signing_key: str = ""
    session_ttl_seconds: int = 900

    emergency_admin_username: str = "emergency-admin"
    emergency_admin_password_hash: SecretStr = SecretStr("")

    oidc_enabled: bool = True
    oidc_issuer: str = ""
    oidc_authorization_endpoint: str = ""
    oidc_token_endpoint: str = ""
    oidc_jwks_uri: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: SecretStr = SecretStr("")
    oidc_redirect_uri: str = "http://localhost:8000/api/v1/auth/oidc/callback"
    web_console_url: str = "http://localhost:3000/"

    def validate_runtime_security(self) -> None:
        key = self.session_signing_key.get_secret_value()
        if len(key) < 32 or key == "development-only-change-me":
            raise RuntimeError("SESSION_SIGNING_KEY must be a non-default secret of 32+ characters")
        if self.oidc_enabled:
            required = {
                "OIDC_ISSUER": self.oidc_issuer,
                "OIDC_AUTHORIZATION_ENDPOINT": self.oidc_authorization_endpoint,
                "OIDC_TOKEN_ENDPOINT": self.oidc_token_endpoint,
                "OIDC_JWKS_URI": self.oidc_jwks_uri,
                "OIDC_CLIENT_ID": self.oidc_client_id,
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise RuntimeError(f"OIDC configuration is incomplete: {', '.join(missing)}")
