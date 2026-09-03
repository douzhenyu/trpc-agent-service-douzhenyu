"""Black-box production-topology smoke test against a disposable Kubernetes cluster."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SMOKE_SCRIPT = REPOSITORY_ROOT / "scripts" / "kubernetes_smoke.sh"


@pytest.mark.smoke
def test_argo_cd_deploys_and_argo_rollouts_rolls_back_healthy_slice() -> None:
    if os.environ.get("RUN_KUBERNETES_SMOKE") != "1":
        pytest.skip("set RUN_KUBERNETES_SMOKE=1 to create the disposable Kind cluster")

    subprocess.run(
        [str(SMOKE_SCRIPT)],
        cwd=REPOSITORY_ROOT,
        env=os.environ.copy(),
        check=True,
        timeout=1_200,
    )
