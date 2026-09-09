"""Black-box production-topology smoke test against a disposable Kubernetes cluster."""

from __future__ import annotations

import os
import signal
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SMOKE_SCRIPT = REPOSITORY_ROOT / "scripts" / "kubernetes_smoke.sh"
REDPANDA_FIXTURE = REPOSITORY_ROOT / "tests" / "deployment" / "fixtures" / "redpanda-smoke.yaml"


def _run_smoke_process(
    script: Path,
    *,
    timeout: float,
    cleanup_timeout: float,
) -> None:
    process = subprocess.Popen(  # noqa: S603
        [str(script)],
        cwd=REPOSITORY_ROOT,
        env=os.environ.copy(),
        start_new_session=True,
    )
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=cleanup_timeout)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        raise

    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, [str(script)])


def test_smoke_timeout_allows_the_shell_exit_trap_to_clean_up(tmp_path: Path) -> None:
    marker = tmp_path / "cleaned"
    script = tmp_path / "wait-for-timeout.sh"
    script.write_text(f"#!/usr/bin/env bash\ntrap 'touch {marker}; exit 143' TERM\nsleep 60\n")
    script.chmod(0o700)

    with pytest.raises(subprocess.TimeoutExpired):
        _run_smoke_process(script, timeout=0.5, cleanup_timeout=1)

    assert marker.exists()


def test_smoke_provisions_the_shared_egress_gateway_before_platform_sync() -> None:
    script = SMOKE_SCRIPT.read_text()

    assert "kube create namespace istio-egress" in script
    assert '--set "components.egressGateways[0].name=istio-egressgateway"' in script
    assert '--set "components.egressGateways[0].namespace=istio-egress"' in script
    assert "kube rollout status deployment/istio-egressgateway -n istio-egress" in script
    assert script.index("kube create namespace istio-egress") < script.index(
        "tests/deployment/fixtures/argocd-smoke.yaml"
    )


def test_smoke_provisions_the_execution_bus_before_platform_sync() -> None:
    script = SMOKE_SCRIPT.read_text()
    resources: dict[tuple[str, str], dict[str, Any]] = {
        (document["kind"], document["metadata"]["name"]): document
        for document in yaml.safe_load_all(REDPANDA_FIXTURE.read_text())
    }

    assert "kube apply -f tests/deployment/fixtures/redpanda-smoke.yaml" in script
    assert "kube rollout status deployment/redpanda -n kafka" in script
    assert script.index("tests/deployment/fixtures/redpanda-smoke.yaml") < script.index(
        "tests/deployment/fixtures/argocd-smoke.yaml"
    )
    assert resources[("Namespace", "kafka")]["metadata"]["name"] == "kafka"
    assert resources[("Service", "redpanda")]["spec"]["ports"] == [
        {"name": "kafka", "port": 9092, "targetPort": "kafka"}
    ]
    redpanda = resources[("Deployment", "redpanda")]["spec"]["template"]
    assert redpanda["metadata"]["labels"] == {"app.kubernetes.io/name": "redpanda"}
    container = redpanda["spec"]["containers"][0]
    assert container["imagePullPolicy"] == "Never"
    assert "redpandadata/redpanda:v24.2.18" in script
    assert container["image"] == "redpandadata/redpanda:v24.2.18"
    assert "--advertise-kafka-addr=redpanda.kafka.svc.cluster.local:9092" in container["args"]


@pytest.mark.smoke
def test_real_ambient_mesh_enforces_zero_trust_and_preserves_safe_rollouts() -> None:
    if os.environ.get("RUN_KUBERNETES_SMOKE") != "1":
        pytest.skip("set RUN_KUBERNETES_SMOKE=1 to create the disposable Kind cluster")

    _run_smoke_process(
        SMOKE_SCRIPT,
        timeout=1_200,
        cleanup_timeout=120,
    )
