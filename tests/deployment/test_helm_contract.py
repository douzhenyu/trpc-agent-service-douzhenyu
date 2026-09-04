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
SMOKE_DATABASE_PATH = REPOSITORY_ROOT / "tests" / "deployment" / "fixtures" / "postgres-smoke.yaml"
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
            manifest["metadata"].get("labels", {}).get("app.kubernetes.io/component"),
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
        assert hpa["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "2"}
        assert hpa["spec"]["scaleTargetRef"] == {
            "apiVersion": ("argoproj.io/v1alpha1" if expected_kind == "Rollout" else "apps/v1"),
            "kind": expected_kind,
            "name": workload["metadata"]["name"],
        }
        assert hpa["spec"]["minReplicas"] >= 2
        assert hpa["spec"]["maxReplicas"] > hpa["spec"]["minReplicas"]

        pdb = resources[("PodDisruptionBudget", unit)]
        assert pdb["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "2"}
        assert pdb["spec"]["maxUnavailable"] == 1


def test_database_owner_and_application_credentials_are_separate_secrets() -> None:
    manifests = render_chart()
    migration = next(manifest for manifest in manifests if manifest["kind"] == "Job")
    admin_api = next(
        manifest
        for manifest in manifests
        if manifest["kind"] == "Deployment"
        and manifest["metadata"]["labels"]["app.kubernetes.io/component"] == "admin-api"
    )

    migration_container = migration["spec"]["template"]["spec"]["containers"][0]
    admin_container = admin_api["spec"]["template"]["spec"]["containers"][0]
    assert migration_container["env"] == [
        {
            "name": "DATABASE_ADMIN_URL",
            "valueFrom": {"secretKeyRef": {"name": "trpc-platform-database-admin", "key": "url"}},
        },
        {
            "name": "DATABASE_APP_PASSWORD",
            "valueFrom": {
                "secretKeyRef": {
                    "name": "trpc-platform-database-admin",
                    "key": "app-password",
                }
            },
        },
    ]
    assert admin_container["env"] == [
        {
            "name": "DATABASE_URL",
            "valueFrom": {"secretKeyRef": {"name": "trpc-platform-database-app", "key": "url"}},
        }
    ]
    assert admin_container["envFrom"] == [{"secretRef": {"name": "trpc-platform-admin-auth"}}]


def test_smoke_database_fixture_satisfies_admin_api_first_install() -> None:
    manifests = list(yaml.safe_load_all(SMOKE_DATABASE_PATH.read_text()))
    resources = {(item["kind"], item["metadata"]["name"]): item for item in manifests}

    assert ("Deployment", "smoke-postgres") in resources
    assert ("Service", "smoke-postgres") in resources
    database = resources[("Deployment", "smoke-postgres")]
    assert database["metadata"]["labels"] == {"trpc-agent-platform.io/database": "true"}
    container = database["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "local/trpc-agent-postgres:smoke"
    assert container["imagePullPolicy"] == "Never"
    database_policy = resources[("NetworkPolicy", "smoke-postgres")]
    assert {"ports": [{"port": 15008, "protocol": "TCP"}]} in database_policy["spec"]["ingress"]
    assert set(resources[("Secret", "trpc-platform-database-admin")]["stringData"]) == {
        "url",
        "app-password",
    }
    assert set(resources[("Secret", "trpc-platform-database-app")]["stringData"]) == {"url"}
    assert {
        "SESSION_SIGNING_KEY",
        "OIDC_ENABLED",
        "SESSION_COOKIE_SECURE",
    } <= set(resources[("Secret", "trpc-platform-admin-auth")]["stringData"])


def test_direct_database_egress_is_opt_in_and_scoped_to_database_pods() -> None:
    manifests = render_chart("--set", "database.direct.enabled=true")
    policies = {
        manifest["metadata"]["labels"].get("app.kubernetes.io/component"): manifest
        for manifest in manifests
        if manifest["kind"] == "NetworkPolicy"
    }

    expected_rule = {
        "to": [{"podSelector": {"matchLabels": {"trpc-agent-platform.io/database": "true"}}}],
        "ports": [
            {"port": 5432, "protocol": "TCP"},
            {"port": 15008, "protocol": "TCP"},
        ],
    }
    assert expected_rule in policies["database-migration"]["spec"]["egress"]
    assert expected_rule in policies["admin-api"]["spec"]["egress"]
    assert expected_rule not in policies["web-console"]["spec"]["egress"]

    waypoint_rule = {
        "to": [
            {
                "podSelector": {
                    "matchLabels": {"gateway.networking.k8s.io/gateway-name": "platform-waypoint"}
                }
            }
        ],
        "ports": [{"port": 15008, "protocol": "TCP"}],
    }
    assert waypoint_rule in policies["database-migration"]["spec"]["egress"]
    assert waypoint_rule in policies["admin-api"]["spec"]["egress"]

    waypoint = next(
        manifest
        for manifest in manifests
        if manifest["kind"] == "NetworkPolicy"
        and manifest["metadata"]["name"] == "platform-waypoint"
    )
    assert {
        "to": [{"podSelector": {"matchLabels": {"trpc-agent-platform.io/database": "true"}}}],
        "ports": [{"port": 15008, "protocol": "TCP"}],
    } in waypoint["spec"]["egress"]


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
            manifest["metadata"].get("labels", {}).get("app.kubernetes.io/component"),
            manifest["metadata"].get("labels", {}).get("app.kubernetes.io/service-role", ""),
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
        expects_kubernetes_auth = unit in {"agent-worker", "agent-gateway"}
        assert service_account["automountServiceAccountToken"] is expects_kubernetes_auth
        assert pod_spec["automountServiceAccountToken"] is expects_kubernetes_auth
        assert pod_spec["securityContext"] == {
            "runAsNonRoot": True,
            "seccompProfile": {"type": "RuntimeDefault"},
        }

        policy_spec = network_policy["spec"]
        assert policy_spec["podSelector"]["matchLabels"] == {
            "trpc-agent-platform.io/network-profile": unit
        }
        assert (
            workload["spec"]["template"]["metadata"]["labels"][
                "trpc-agent-platform.io/network-profile"
            ]
            == unit
        )
        assert policy_spec["policyTypes"] == ["Ingress", "Egress"]
        assert any(
            port == {"port": 15008, "protocol": "TCP"}
            for rule in policy_spec["ingress"]
            for port in rule.get("ports", [])
        )
        assert any(
            port == {"port": 53, "protocol": "UDP"}
            for rule in policy_spec["egress"]
            for port in rule.get("ports", [])
        )

    agent_worker = resources[("Rollout", "agent-worker")]
    container = agent_worker["spec"]["template"]["spec"]["containers"][0]
    assert container["args"][0] == "trpc_service.agent_worker:app"
    assert {item["name"] for item in container["env"]} >= {
        "DATABASE_URL",
        "LLM_GATEWAY_URL",
    }
    agent_gateway = resources[("Rollout", "agent-gateway")]
    gateway_container = agent_gateway["spec"]["template"]["spec"]["containers"][0]
    assert gateway_container["args"][0] == "trpc_service.llm_gateway:app"
    assert {item["name"] for item in gateway_container["env"]} >= {
        "DATABASE_URL",
        "VAULT_URL",
        "VAULT_KUBERNETES_ROLE",
        "OPA_URL",
    }


def test_chart_owns_namespace_resource_governance_and_a_sync_migration_job() -> None:
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
        "argocd.argoproj.io/hook": "Sync",
        "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation,HookSucceeded",
        "argocd.argoproj.io/sync-wave": "0",
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
