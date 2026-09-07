"""Upstream stable-release discovery and supply-chain workflow contract."""

from __future__ import annotations

from pathlib import Path

from trpc_service.upstream import is_stable_version, latest_stable, parse_version

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_parse_version_accepts_only_exact_stable_semver() -> None:
    assert parse_version("1.2.3") == (1, 2, 3)
    assert parse_version("1.2.3rc1") is None
    assert parse_version("1.2.3.post0") is None
    assert parse_version("1.2.3.dev0") is None
    assert parse_version("1.2") is None
    assert parse_version("") is None
    assert is_stable_version("2.0.14")
    assert not is_stable_version("2.0.14b1")


def test_latest_stable_skips_prereleases_and_picks_newest() -> None:
    releases = ["1.0.0", "1.1.0", "2.0.0rc1", "2.0.0b2", "1.2.0", "3.0.0.dev1"]
    assert latest_stable(releases, "1.1.0") == "1.2.0"
    assert latest_stable(releases, "1.2.0") is None
    assert latest_stable(releases, "0.9.0") == "1.2.0"
    # Unparseable current pin: refuse to propose anything.
    assert latest_stable(releases, "unknown") is None
    # Empty release list: no candidate.
    assert latest_stable([], "1.0.0") is None


def test_upstream_upgrade_workflow_creates_gated_pr() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/upstream-upgrade.yml").read_text()
    assert "schedule:" in workflow and "cron:" in workflow
    assert "trpc-agent-py==" in workflow
    assert "PINNED_TRPC_AGENT_VERSION" in workflow
    assert "docs/release-promotion-gates.md" in workflow
    assert "Full regression suite" in workflow
    assert "canary" in workflow.lower()


def test_ci_carries_supply_chain_gates() -> None:
    workflow = (REPOSITORY_ROOT / ".github/workflows/ci.yml").read_text()
    assert "supply-chain" in workflow
    assert "uv sync --locked" in workflow
    assert "cyclonedx-json" in workflow
    assert "pip-audit" in workflow
    assert "gitleaks" in workflow
    assert "npm audit --omit=dev --audit-level=high" in workflow
    # Coverage gates stay in place.
    assert "--cov-fail-under=80" in workflow
    assert "--fail-under=90" in workflow


def test_promotion_ladder_documented() -> None:
    document = (REPOSITORY_ROOT / "docs/release-promotion-gates.md").read_text()
    for gate in ("完整回归", "Staging", "灰度", "RPO", "SBOM"):
        assert gate in document
