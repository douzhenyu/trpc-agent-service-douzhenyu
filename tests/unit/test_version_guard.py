import pytest

from trpc_service.version import require_pinned_trpc_agent_version


def test_pinned_sdk_version_is_accepted() -> None:
    assert require_pinned_trpc_agent_version("1.1.19") == "1.1.19"


def test_unreviewed_sdk_version_blocks_startup() -> None:
    with pytest.raises(
        RuntimeError,
        match=r"expected 1\.1\.19, got 1\.2\.0",
    ):
        require_pinned_trpc_agent_version("1.2.0")
