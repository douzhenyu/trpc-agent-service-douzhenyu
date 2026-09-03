"""Package and upstream SDK versions exposed by public service boundaries."""

from importlib.metadata import version

__version__ = "0.1.0"
PINNED_TRPC_AGENT_VERSION = "1.1.19"


def require_pinned_trpc_agent_version(installed_version: str) -> str:
    """Fail startup when the runtime SDK differs from the reviewed lock baseline."""
    if installed_version != PINNED_TRPC_AGENT_VERSION:
        raise RuntimeError(
            "unsupported trpc-agent-py runtime: "
            f"expected {PINNED_TRPC_AGENT_VERSION}, got {installed_version}"
        )
    return installed_version


TRPC_AGENT_VERSION = require_pinned_trpc_agent_version(version("trpc-agent-py"))
