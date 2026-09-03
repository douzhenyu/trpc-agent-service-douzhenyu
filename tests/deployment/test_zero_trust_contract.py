"""Public zero-trust contract rendered by the production Helm chart."""

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


def render_chart(*extra_args: str) -> list[dict[str, Any]]:
    helm = os.environ.get("HELM_BIN", "helm")
    completed = subprocess.run(
        [helm, "template", "platform", str(CHART_PATH), *extra_args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [document for document in yaml.safe_load_all(completed.stdout) if document]


def test_production_namespace_and_workloads_have_stable_ambient_identities() -> None:
    manifests = render_chart()
    service_accounts = {
        manifest["metadata"]["labels"]["app.kubernetes.io/component"]: manifest
        for manifest in manifests
        if manifest["kind"] == "ServiceAccount"
        and manifest["metadata"]["labels"].get("app.kubernetes.io/component") in EXPECTED_UNITS
    }
    workloads = {
        manifest["metadata"]["labels"]["app.kubernetes.io/component"]: manifest
        for manifest in manifests
        if manifest["kind"] in {"Deployment", "Rollout"}
    }

    assert set(service_accounts) == EXPECTED_UNITS
    for unit in EXPECTED_UNITS:
        assert service_accounts[unit]["metadata"]["name"] == unit
        assert workloads[unit]["spec"]["template"]["spec"]["serviceAccountName"] == unit
        assert workloads[unit]["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "1"}

    services = [manifest for manifest in manifests if manifest["kind"] == "Service"]
    for service in services:
        assert service["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "-3"}

    application_set = yaml.safe_load(APPLICATION_SET_PATH.read_text())
    namespace_labels = application_set["spec"]["template"]["spec"]["syncPolicy"][
        "managedNamespaceMetadata"
    ]["labels"]
    assert namespace_labels == {
        "istio.io/dataplane-mode": "ambient",
        "istio.io/ingress-use-waypoint": "true",
        "istio.io/use-waypoint": "platform-waypoint",
    }

    peer_authentications = [
        manifest for manifest in manifests if manifest["kind"] == "PeerAuthentication"
    ]
    assert peer_authentications == [
        {
            "apiVersion": "security.istio.io/v1",
            "kind": "PeerAuthentication",
            "metadata": {
                "name": "platform-strict-mtls",
                "annotations": {
                    "argocd.argoproj.io/sync-wave": "-4",
                },
            },
            "spec": {"mtls": {"mode": "STRICT"}},
        }
    ]

    waypoint = next(
        manifest
        for manifest in manifests
        if manifest["kind"] == "Gateway" and manifest["metadata"]["name"] == "platform-waypoint"
    )
    assert waypoint["spec"] == {
        "gatewayClassName": "istio-waypoint",
        "listeners": [
            {
                "name": "mesh",
                "port": 15008,
                "protocol": "HBONE",
                "allowedRoutes": {"namespaces": {"from": "Same"}},
            }
        ],
    }


def test_authorization_is_default_deny_and_only_allows_declared_l7_calls() -> None:
    manifests = render_chart()
    policies = {
        manifest["metadata"]["name"]: manifest
        for manifest in manifests
        if manifest["kind"] == "AuthorizationPolicy"
    }

    services_by_unit: dict[str, set[str]] = {unit: set() for unit in EXPECTED_UNITS}
    for manifest in manifests:
        if manifest["kind"] == "Service":
            services_by_unit[manifest["metadata"]["labels"]["app.kubernetes.io/component"]].add(
                manifest["metadata"]["name"]
            )

    for unit in EXPECTED_UNITS:
        require_waypoint = policies[f"{unit}-require-waypoint"]
        assert require_waypoint["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "-2"}
        assert require_waypoint["spec"] == {
            "selector": {"matchLabels": {"app.kubernetes.io/component": unit}},
            "action": "ALLOW",
            "rules": [
                {
                    "from": [
                        {
                            "source": {
                                "principals": ["cluster.local/ns/default/sa/platform-waypoint"]
                            }
                        }
                    ]
                }
            ],
        }

        allow = policies[f"{unit}-allow-declared-calls"]["spec"]
        assert allow["action"] == "ALLOW"
        assert {target_ref["name"] for target_ref in allow["targetRefs"]} == services_by_unit[unit]
        for rule in allow["rules"]:
            assert rule["from"][0]["source"]["serviceAccounts"]
            operation = rule["to"][0]["operation"]
            assert operation["methods"]
            assert operation["paths"]

        deny_forged_tenant = policies[f"{unit}-deny-unsigned-tenant-context"]["spec"]
        assert policies[f"{unit}-allow-declared-calls"]["metadata"]["annotations"] == {
            "argocd.argoproj.io/sync-wave": "-2"
        }
        assert policies[f"{unit}-deny-unsigned-tenant-context"]["metadata"]["annotations"] == {
            "argocd.argoproj.io/sync-wave": "-2"
        }
        assert deny_forged_tenant["action"] == "DENY"
        assert deny_forged_tenant["targetRefs"] == allow["targetRefs"]
        assert deny_forged_tenant["rules"] == [
            {
                "when": [
                    {
                        "key": "request.headers[x-tenant-id]",
                        "values": ["*"],
                    }
                ]
            }
        ]

    admin_allow = policies["admin-api-allow-declared-calls"]["spec"]["rules"]
    assert admin_allow == [
        {
            "from": [{"source": {"serviceAccounts": ["default/web-console"]}}],
            "to": [
                {
                    "operation": {
                        "methods": ["GET"],
                        "paths": ["/api/v1/health"],
                    }
                }
            ],
        }
    ]


def test_network_policy_only_opens_ambient_health_dns_declared_and_gateway_paths() -> None:
    manifests = render_chart()
    baseline = next(
        manifest
        for manifest in manifests
        if manifest["kind"] == "NetworkPolicy"
        and manifest["metadata"]["name"] == "platform-workloads-default-deny"
    )
    assert baseline["spec"] == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}
    assert baseline["metadata"]["annotations"] == {
        "argocd.argoproj.io/sync-wave": "-3",
    }

    migration = next(manifest for manifest in manifests if manifest["kind"] == "Job")
    assert (
        migration["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/name"]
        == "trpc-agent-platform"
    )

    waypoint_policy = next(
        manifest
        for manifest in manifests
        if manifest["kind"] == "NetworkPolicy"
        and manifest["metadata"]["name"] == "platform-waypoint"
    )
    assert waypoint_policy["spec"]["podSelector"] == {
        "matchLabels": {"gateway.networking.k8s.io/gateway-name": "platform-waypoint"}
    }
    assert waypoint_policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert waypoint_policy["spec"]["ingress"] == [
        {"ports": [{"port": 15008, "protocol": "TCP"}]},
        {
            "from": [
                {"ipBlock": {"cidr": "169.254.7.127/32"}},
                {"ipBlock": {"cidr": "fd16:9254:7127:1337:ffff:ffff:ffff:ffff/128"}},
            ],
            "ports": [{"port": 15021, "protocol": "TCP"}],
        },
    ]

    policies = {
        manifest["metadata"]["labels"]["app.kubernetes.io/component"]: manifest["spec"]
        for manifest in manifests
        if manifest["kind"] == "NetworkPolicy"
        and "app.kubernetes.io/component" in manifest["metadata"]["labels"]
    }

    for unit in EXPECTED_UNITS:
        policy_manifest = next(
            manifest
            for manifest in manifests
            if manifest["kind"] == "NetworkPolicy"
            and manifest["metadata"]["labels"].get("app.kubernetes.io/component") == unit
        )
        assert policy_manifest["metadata"]["annotations"] == {"argocd.argoproj.io/sync-wave": "-1"}
        policy = policies[unit]
        assert policy["podSelector"] == {
            "matchLabels": {"trpc-agent-platform.io/network-profile": unit}
        }
        assert policy["policyTypes"] == ["Ingress", "Egress"]
        assert policy["ingress"] == [
            {"ports": [{"port": 15008, "protocol": "TCP"}]},
            {
                "from": [
                    {"ipBlock": {"cidr": "169.254.7.127/32"}},
                    {"ipBlock": {"cidr": "fd16:9254:7127:1337:ffff:ffff:ffff:ffff/128"}},
                ],
                "ports": [
                    {
                        "port": 8000 if unit != "web-console" else 8080,
                        "protocol": "TCP",
                    }
                ],
            },
        ]

        assert policy["egress"][0] == {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    }
                }
            ],
            "ports": [
                {"port": 53, "protocol": "UDP"},
                {"port": 53, "protocol": "TCP"},
            ],
        }
        assert policy["egress"][-1] == {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "istio-egress"}
                    },
                    "podSelector": {"matchLabels": {"istio": "egressgateway"}},
                }
            ],
            "ports": [{"port": 15008, "protocol": "TCP"}],
        }

    assert policies["web-console"]["egress"][1] == {
        "to": [
            {
                "podSelector": {
                    "matchLabels": {"gateway.networking.k8s.io/gateway-name": "platform-waypoint"}
                }
            }
        ],
        "ports": [{"port": 15008, "protocol": "TCP"}],
    }
    for unit in EXPECTED_UNITS - {"web-console"}:
        assert len(policies[unit]["egress"]) == 2


def test_independent_releases_have_one_shared_mesh_owner_and_unit_scoped_policy() -> None:
    for unit in EXPECTED_UNITS:
        values_file = (
            REPOSITORY_ROOT / "deploy" / "gitops" / "production" / "values" / f"{unit}.yaml"
        )
        manifests = render_chart("--values", str(values_file))

        assert sum(manifest["kind"] == "PeerAuthentication" for manifest in manifests) == (
            unit == "admin-api"
        )
        assert sum(manifest["kind"] == "Gateway" for manifest in manifests) == (unit == "admin-api")
        assert sum(
            manifest["kind"] == "NetworkPolicy"
            and manifest["metadata"]["name"] == "platform-workloads-default-deny"
            for manifest in manifests
        ) == (unit == "admin-api")
        assert sum(
            manifest["kind"] == "NetworkPolicy"
            and manifest["metadata"]["name"] == "platform-waypoint"
            for manifest in manifests
        ) == (unit == "admin-api")
        policies = [manifest for manifest in manifests if manifest["kind"] == "AuthorizationPolicy"]
        assert {policy["metadata"]["name"] for policy in policies} == {
            f"{unit}-require-waypoint",
            f"{unit}-allow-declared-calls",
            f"{unit}-deny-unsigned-tenant-context",
        }


def test_workload_principals_use_the_configured_trust_domain() -> None:
    manifests = render_chart("--set", "zeroTrust.trustDomain=prod.example")
    policies = [
        manifest
        for manifest in manifests
        if manifest["kind"] == "AuthorizationPolicy"
        and manifest["metadata"]["name"].endswith("-require-waypoint")
    ]

    for policy in policies:
        assert policy["spec"]["rules"][0]["from"][0]["source"]["principals"] == [
            "prod.example/ns/default/sa/platform-waypoint"
        ]
