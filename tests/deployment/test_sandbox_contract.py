"""Sandbox contract rendered by the production Helm chart."""

from __future__ import annotations

from typing import Any

from tests.deployment.test_zero_trust_contract import render_chart


def _sandbox_manifests() -> list[dict[str, Any]]:
    return [
        manifest
        for manifest in render_chart()
        if manifest.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        == "code-sandbox"
        or manifest.get("metadata", {}).get("name", "").endswith("sandbox-deny-egress")
    ]


def test_chart_ships_a_gvisor_runtime_class() -> None:
    runtime_classes = [
        manifest for manifest in _sandbox_manifests() if manifest["kind"] == "RuntimeClass"
    ]
    assert len(runtime_classes) == 1
    runtime_class = runtime_classes[0]
    assert runtime_class["metadata"]["name"] == "gvisor"
    assert runtime_class["handler"] == "runsc"


def test_sandbox_pods_have_no_egress_by_default() -> None:
    policies = [
        manifest for manifest in _sandbox_manifests() if manifest["kind"] == "NetworkPolicy"
    ]
    assert len(policies) == 1
    spec = policies[0]["spec"]
    assert spec["podSelector"]["matchLabels"] == {"platform.trpc/sandbox": "true"}
    assert spec["policyTypes"] == ["Egress"]
    assert spec["egress"] == []


def test_no_workload_mounts_the_docker_socket() -> None:
    for manifest in render_chart():
        pod_spec = (manifest.get("spec") or {}).get("template", {}).get("spec") or {}
        volumes = pod_spec.get("volumes") or []
        for volume in volumes:
            host_path = volume.get("hostPath", {}).get("path", "")
            assert "docker.sock" not in host_path, (
                f"{manifest['metadata']['name']} mounts docker socket"
            )
