"""Repository-level contract for Issue #38 final acceptance evidence."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_readme_navigates_every_required_deliverable() -> None:
    readme = (ROOT / "README.md").read_text()
    expected_links = {
        "架构设计文档": "docs/architecture/architecture-design.md",
        "系统架构图": "docs/architecture/system-diagrams.md",
        "核心时序图": "docs/architecture/system-diagrams.md",
        "数据模型设计": "docs/architecture/data-model-and-sync.md",
        "数据同步和幂等策略": "docs/architecture/data-model-and-sync.md",
        "多后端适配方案": "docs/architecture/data-model-and-sync.md",
        "生产风险清单": "docs/risk-register-and-acceptance.md",
        "GitHub 实现代码": "docs/final-acceptance.md",
    }
    for label, target in expected_links.items():
        assert f"[{label}]({target})" in readme


def test_final_acceptance_binds_risks_and_clean_environment_evidence() -> None:
    evidence = (ROOT / "docs/final-acceptance.md").read_text()
    for risk_id in range(1, 17):
        assert f"R-{risk_id:02d}" in evidence
    assert "public-boundary-smoke" in evidence
    assert "kubernetes-production-smoke" in evidence
    assert "uv run pytest tests/unit tests/integration" in evidence
    assert "npm run test:smoke" in evidence
    assert "不得将未运行的生产演练写为通过" in evidence
