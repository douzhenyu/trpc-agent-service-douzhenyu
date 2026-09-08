"""Observability asset contract: SLO dashboards, alert rules and chart wiring."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHART_PATH = REPOSITORY_ROOT / "deploy" / "helm" / "trpc-agent-platform"
OBSERVABILITY_PATH = CHART_PATH / "observability"


def _dashboard(name: str) -> dict:
    document = json.loads((OBSERVABILITY_PATH / "dashboards" / f"{name}.json").read_text())
    assert document["schemaVersion"] >= 39
    return document


def test_data_plane_dashboard_carries_99_95_objective() -> None:
    dashboard = _dashboard("slo-data-plane")
    availability = dashboard["panels"][0]
    assert "99.95%" in availability["title"]
    expr = availability["targets"][0]["expr"]
    assert "agent-gateway" in expr and "channel-gateway" in expr
    assert "platform_http_requests_total" in expr
    burn = dashboard["panels"][1]["targets"][0]["expr"]
    assert "/ 0.0005" in burn


def test_control_plane_dashboard_carries_99_9_objective() -> None:
    dashboard = _dashboard("slo-control-plane")
    expr = dashboard["panels"][0]["targets"][0]["expr"]
    assert 'service="admin-api"' in expr
    burn = dashboard["panels"][1]["targets"][0]["expr"]
    assert "/ 0.001" in burn


def test_alert_rules_cover_burn_rate_and_pipeline() -> None:
    document = yaml.safe_load(
        (OBSERVABILITY_PATH / "alerts" / "platform-slo-alerts.yaml").read_text()
    )
    groups = document["groups"]
    alerts = {rule["alert"]: rule for group in groups for rule in group["rules"]}
    assert {
        "DataPlaneSLOBurnRateFast",
        "DataPlaneSLOBurnRateSlow",
        "ControlPlaneSLOBurnRateFast",
        "ControlPlaneSLOBurnRateSlow",
        "ExecutionPipelineFailures",
    } <= set(alerts)
    for name in ("DataPlaneSLOBurnRateFast", "ControlPlaneSLOBurnRateFast"):
        rule = alerts[name]
        assert rule["labels"]["severity"] == "critical"
        assert "annotations" in rule
        assert "14.4" in rule["expr"].replace("\n", "")


def test_chart_exposes_observability_switches() -> None:
    values = yaml.safe_load((CHART_PATH / "values.yaml").read_text())
    assert values["observability"]["dashboards"]["enabled"] is False
    assert values["observability"]["prometheusRule"]["enabled"] is False

    template = (CHART_PATH / "templates" / "observability.yaml").read_text()
    assert "{{- if .Values.observability.dashboards.enabled }}" in template
    assert "{{- if .Values.observability.prometheusRule.enabled }}" in template
    assert '.Files.Get "observability/dashboards/slo-data-plane.json"' in template
    assert "grafana_dashboard" in template
    assert "monitoring.coreos.com/v1" in template
