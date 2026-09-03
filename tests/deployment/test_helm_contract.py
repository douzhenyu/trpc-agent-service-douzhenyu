"""Public Helm rendering contract for the six production deployment units."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHART_PATH = REPOSITORY_ROOT / "deploy" / "helm" / "trpc-agent-platform"
APPLICATION_SET_PATH = REPOSITORY_ROOT / "deploy" / "gitops" / "production" / "applicationset.yaml"
EXPECTED_UNITS = {
    "admin-api",
    "web-console",
    "agent-gateway",
    "channel-gateway",
    "agent-worker",
    "job-worker",
}
ROLLOUT_UNITS = {
    "agent-gateway",
    "channel-gateway",
    "agent-worker",
    "job-worker",
}


def render_chart(*extra_args: str) -> list[dict[str, Any]]:
    """Render the public chart exactly as an operator would before installation."""
    helm = os.environ.get("HELM_BIN", "helm")
    completed = subprocess.run(
        [helm, "template", "platform", str(CHART_PATH), *extra_args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [document for document in yaml.safe_load_all(completed.stdout) if document]


def test_chart_renders_all_six_production_units() -> None:
    manifests = render_chart()

    workloads = [
        manifest for manifest in manifests if manifest["kind"] in {"Deployment", "Rollout"}
    ]

    assert {
        workload["metadata"]["labels"]["app.kubernetes.io/component"] for workload in workloads
    } == EXPECTED_UNITS
    assert len(workloads) == len(EXPECTED_UNITS)


def test_each_unit_has_production_health_scaling_and_scheduling_policy() -> None:
    manifests = render_chart()
    resources = {
        (
            manifest["kind"],
            manifest["metadata"]["labels"].get("app.kubernetes.io/component"),
        ): manifest
        for manifest in manifests
    }

    for unit in EXPECTED_UNITS:
        expected_kind = "Rollout" if unit in ROLLOUT_UNITS else "Deployment"
        workload = resources[(expected_kind, unit)]
        container = workload["spec"]["template"]["spec"]["containers"][0]

        assert "replicas" not in workload["spec"]
        assert container["image"].count("@sha256:") == 1
        assert container["livenessProbe"]["httpGet"]["path"]
        assert container["readinessProbe"]["httpGet"]["path"]
        assert container["resources"]["requests"] == {"cpu": "100m", "memory": "128Mi"}
        assert container["resources"]["limits"] == {"cpu": "1", "memory": "512Mi"}

        topology = workload["spec"]["template"]["spec"]["topologySpreadConstraints"]
        assert {constraint["topologyKey"] for constraint in topology} == {
            "kubernetes.io/hostname",
            "topology.kubernetes.io/zone",
        }
        zone_constraint = next(
            constraint
            for constraint in topology
            if constraint["topologyKey"] == "topology.kubernetes.io/zone"
        )
        assert zone_constraint["minDomains"] == 3

        hpa = resources[("HorizontalPodAutoscaler", unit)]
        assert hpa["spec"]["scaleTargetRef"] == {
            "apiVersion": ("argoproj.io/v1alpha1" if expected_kind == "Rollout" else "apps/v1"),
            "kind": expected_kind,
            "name": workload["metadata"]["name"],
        }
        assert hpa["spec"]["minReplicas"] >= 2
        assert hpa["spec"]["maxReplicas"] > hpa["spec"]["minReplicas"]

        pdb = resources[("PodDisruptionBudget", unit)]
        assert pdb["spec"]["maxUnavailable"] == 1


def test_argo_cd_declares_each_unit_as_an_independent_helm_release() -> None:
    application_set = yaml.safe_load(APPLICATION_SET_PATH.read_text())
    elements = application_set["spec"]["generators"][0]["list"]["elements"]

    assert {element["unit"] for element in elements} == EXPECTED_UNITS
    assert next(element for element in elements if element["unit"] == "admin-api")["syncPhase"] == (
        "migration-gate"
    )
    assert {element["syncPhase"] for element in elements if element["unit"] != "admin-api"} == {
        "workloads"
    }
    assert (
        application_set["spec"]["template"]["metadata"]["labels"][
            "trpc-agent-platform.io/sync-phase"
        ]
        == "{{syncPhase}}"
    )
    assert application_set["spec"]["strategy"] == {
        "type": "RollingSync",
        "rollingSync": {
            "steps": [
                {
                    "matchExpressions": [
                        {
                            "key": "trpc-agent-platform.io/sync-phase",
                            "operator": "In",
                            "values": ["migration-gate"],
                        }
                    ],
                    "maxUpdate": 1,
                },
                {
                    "matchExpressions": [
                        {
                            "key": "trpc-agent-platform.io/sync-phase",
                            "operator": "In",
                            "values": ["workloads"],
                        }
                    ]
                },
            ]
        },
    }
    assert application_set["spec"]["template"]["spec"]["source"] == {
        "repoURL": "https://github.com/douzhenyu/trpc-agent-service-douzhenyu.git",
        "targetRevision": "main",
        "path": "deploy/helm/trpc-agent-platform",
        "helm": {
            "valueFiles": ["../../gitops/production/values/{{unit}}.yaml"],
        },
    }
    sync_policy = application_set["spec"]["template"]["spec"]["syncPolicy"]
    assert sync_policy["automated"] == {"prune": True, "selfHeal": True}

    for unit in EXPECTED_UNITS:
        values_file = (
            REPOSITORY_ROOT / "deploy" / "gitops" / "production" / "values" / f"{unit}.yaml"
        )
        manifests = render_chart("--values", str(values_file))
        workloads = [
            manifest for manifest in manifests if manifest["kind"] in {"Deployment", "Rollout"}
        ]
        assert [
            workload["metadata"]["labels"]["app.kubernetes.io/component"] for workload in workloads
        ] == [unit]


def test_rollouts_gate_canaries_on_health_and_keep_rollback_history() -> None:
    manifests = render_chart()
    resources = {
        (
            manifest["kind"],
            manifest["metadata"]["labels"].get("app.kubernetes.io/component"),
            manifest["metadata"]["labels"].get("app.kubernetes.io/service-role", ""),
        ): manifest
        for manifest in manifests
    }

    for unit in ROLLOUT_UNITS:
        rollout = resources[("Rollout", unit, "")]
        canary = rollout["spec"]["strategy"]["canary"]
        assert rollout["spec"]["rollbackWindow"]["revisions"] == 3
        assert canary["stableService"] == f"{unit}-stable"
        assert canary["canaryService"] == f"{unit}-canary"

        analysis_steps = [step["analysis"] for step in canary["steps"] if "analysis" in step]
        assert analysis_steps == [
            {
                "templates": [{"templateName": f"platform-trpc-agent-platform-{unit}"}],
                "args": [
                    {
                        "name": "health-url",
                        "value": f"http://{unit}-canary.default.svc:8000/health/ready",
                    },
                    {"name": "expected-service", "value": unit},
                ],
            }
        ]

        assert resources[("Service", unit, "stable")]["metadata"]["name"] == f"{unit}-stable"
        assert resources[("Service", unit, "canary")]["metadata"]["name"] == f"{unit}-canary"

        analysis = resources[("AnalysisTemplate", unit, "")]
        metric = analysis["spec"]["metrics"][0]
        assert metric["successCondition"] == (
            "result.statusCode == 200 && result.body.status == 'ok' "
            "&& result.body.service == '{{args.expected-service}}'"
        )
        assert metric["provider"]["web"]["url"] == "{{args.health-url}}"


def test_each_unit_has_a_dedicated_identity_and_network_boundary() -> None:
    manifests = render_chart()
    resources = {
        (
            manifest["kind"],
            manifest["metadata"]["labels"].get("app.kubernetes.io/component"),
        ): manifest
        for manifest in manifests
        if manifest["kind"] in {"Deployment", "Rollout", "ServiceAccount", "NetworkPolicy"}
    }

    for unit in EXPECTED_UNITS:
        workload_kind = "Rollout" if unit in ROLLOUT_UNITS else "Deployment"
        workload = resources[(workload_kind, unit)]
        pod_spec = workload["spec"]["template"]["spec"]
        service_account = resources[("ServiceAccount", unit)]
        network_policy = resources[("NetworkPolicy", unit)]

        assert pod_spec["serviceAccountName"] == service_account["metadata"]["name"]
        assert service_account["automountServiceAccountToken"] is False
        assert pod_spec["automountServiceAccountToken"] is False
        assert pod_spec["securityContext"] == {
            "runAsNonRoot": True,
            "seccompProfile": {"type": "RuntimeDefault"},
        }

        policy_spec = network_policy["spec"]
        assert policy_spec["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == unit
        assert policy_spec["policyTypes"] == ["Ingress", "Egress"]
        assert any(
            peer.get("namespaceSelector", {})
            .get("matchLabels", {})
            .get("kubernetes.io/metadata.name")
            == "argo-rollouts"
            for rule in policy_spec["ingress"]
            for peer in rule["from"]
        )
        assert any(
            port == {"port": 53, "protocol": "UDP"}
            for rule in policy_spec["egress"]
            for port in rule.get("ports", [])
        )


def test_chart_owns_namespace_resource_governance_and_a_presync_migration_job() -> None:
    manifests = render_chart()

    quotas = [manifest for manifest in manifests if manifest["kind"] == "ResourceQuota"]
    assert len(quotas) == 1
    assert quotas[0]["spec"]["hard"] == {
        "pods": "100",
        "requests.cpu": "20",
        "requests.memory": "40Gi",
        "limits.cpu": "100",
        "limits.memory": "100Gi",
    }

    jobs = [manifest for manifest in manifests if manifest["kind"] == "Job"]
    assert len(jobs) == 1
    job = jobs[0]
    assert job["metadata"]["annotations"] == {
        "argocd.argoproj.io/hook": "PreSync",
        "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation,HookSucceeded",
    }
    assert job["spec"]["backoffLimit"] == 1
    migration_container = job["spec"]["template"]["spec"]["containers"][0]
    assert migration_container["image"].count("@sha256:") == 1
    assert migration_container["command"] == ["/app/.venv/bin/python"]
    assert migration_container["args"] == ["-m", "trpc_service.database_migrations"]

    for unit in EXPECTED_UNITS:
        values_file = (
            REPOSITORY_ROOT / "deploy" / "gitops" / "production" / "values" / f"{unit}.yaml"
        )
        unit_manifests = render_chart("--values", str(values_file))
        assert sum(manifest["kind"] == "ResourceQuota" for manifest in unit_manifests) == (
            unit == "admin-api"
        )
        assert sum(manifest["kind"] == "Job" for manifest in unit_manifests) == (
            unit == "admin-api"
        )
