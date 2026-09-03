"""Black-box production-topology smoke test against a disposable Kubernetes cluster."""

from __future__ import annotations

import os
import signal
import subprocess
from contextlib import suppress
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SMOKE_SCRIPT = REPOSITORY_ROOT / "scripts" / "kubernetes_smoke.sh"


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


@pytest.mark.smoke
def test_real_ambient_mesh_enforces_zero_trust_and_preserves_safe_rollouts() -> None:
    if os.environ.get("RUN_KUBERNETES_SMOKE") != "1":
        pytest.skip("set RUN_KUBERNETES_SMOKE=1 to create the disposable Kind cluster")

    _run_smoke_process(
        SMOKE_SCRIPT,
        timeout=1_200,
        cleanup_timeout=120,
    )
