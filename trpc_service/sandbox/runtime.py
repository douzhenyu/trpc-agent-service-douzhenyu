"""Sandbox runtime boundary: submit code to the isolated gVisor pod runner.

The runtime client owns every Kubernetes interaction. Without a usable
runtime (no in-cluster identity, unreachable API) it raises
SANDBOX_UNAVAILABLE and the executor fails closed — untrusted code is never
run anywhere less isolated than the sandbox.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx

from trpc_service.sandbox.errors import SandboxError
from trpc_service.sandbox.policy import (
    SandboxArtifact,
    SandboxPolicy,
    encode_artifact,
    sandbox_pod_spec,
)

__all__ = ["SandboxError", "SandboxExecutionResult", "SandboxRuntime", "KubernetesSandboxRuntime"]


@dataclass(frozen=True)
class SandboxExecutionResult:
    ok: bool
    exit_code: int
    output: str
    truncated: bool
    timed_out: bool = False


class SandboxRuntime(Protocol):
    async def run(
        self,
        policy: SandboxPolicy,
        code: str,
        input_files: list[tuple[str, bytes]] | None = None,
    ) -> SandboxExecutionResult: ...


@dataclass(frozen=True)
class _ClusterConfig:
    api_url: str
    token: str
    verify: ssl.SSLContext | bool


def in_cluster_config() -> _ClusterConfig | None:
    """Service-account identity when running inside the cluster; else None."""

    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    if not host or not token_path.exists():
        return None
    context = ssl.create_default_context(cafile=str(ca_path)) if ca_path.exists() else True
    return _ClusterConfig(
        api_url=f"https://{host}:{port}",
        token=token_path.read_text(encoding="utf-8").strip(),
        verify=context,
    )


class KubernetesSandboxRuntime:
    """Creates one hardened sandbox pod per execution and collects its output.

    The pod carries the code and input artifacts via environment payload and
    is deleted again right after the logs are read, so every execution gets a
    fresh, isolated pod and nothing outlives the call.
    """

    def __init__(self, *, namespace: str = "platform", poll_interval: float = 0.2) -> None:
        self._namespace = namespace
        self._poll_interval = poll_interval

    def _config(self) -> _ClusterConfig:
        config = in_cluster_config()
        if config is None:
            raise SandboxError("SANDBOX_UNAVAILABLE")
        return config

    async def run(
        self,
        policy: SandboxPolicy,
        code: str,
        input_files: list[tuple[str, bytes]] | None = None,
    ) -> SandboxExecutionResult:
        config = self._config()
        from uuid import uuid4

        execution_id = str(uuid4())
        spec = sandbox_pod_spec(policy, execution_id=execution_id)
        payload = {
            "code": code,
            "inputs": [
                {
                    "name": name,
                    "content": encode_artifact(SandboxArtifact.from_bytes(name, content)),
                }
                for name, content in (input_files or [])
            ],
        }
        spec["spec"]["containers"][0]["env"].append(
            {"name": "SANDBOX_PAYLOAD", "value": json.dumps(payload)}
        )
        async with httpx.AsyncClient(
            base_url=config.api_url, verify=config.verify, timeout=30.0
        ) as client:
            headers = {"Authorization": f"Bearer {config.token}"}
            created = await client.post(
                f"/api/v1/namespaces/{self._namespace}/pods",
                json=spec,
                headers=headers,
            )
            if created.status_code not in (200, 201):
                raise SandboxError("SANDBOX_UNAVAILABLE")
            try:
                return await self._await_completion(client, headers, execution_id, policy)
            finally:
                await client.delete(
                    f"/api/v1/namespaces/{self._namespace}/pods/sandbox-{execution_id[:32]}",
                    headers=headers,
                )

    async def _await_completion(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        execution_id: str,
        policy: SandboxPolicy,
    ) -> SandboxExecutionResult:
        pod_name = f"sandbox-{execution_id[:32]}"
        deadline = asyncio.get_event_loop().time() + policy.execution_timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            pod = await client.get(
                f"/api/v1/namespaces/{self._namespace}/pods/{pod_name}", headers=headers
            )
            if pod.status_code == 200:
                status: dict[str, Any] = pod.json().get("status", {})
                if status.get("phase") in ("Succeeded", "Failed"):
                    logs = await client.get(
                        f"/api/v1/namespaces/{self._namespace}/pods/{pod_name}/log",
                        headers=headers,
                    )
                    output = logs.text if logs.status_code == 200 else ""
                    exit_code = _exit_code(status)
                    return SandboxExecutionResult(
                        ok=exit_code == 0,
                        exit_code=exit_code,
                        output=output,
                        truncated=False,
                    )
            await asyncio.sleep(self._poll_interval)
        return SandboxExecutionResult(
            ok=False, exit_code=137, output="", truncated=False, timed_out=True
        )


def _exit_code(status: dict[str, Any]) -> int:
    for condition in status.get("containerStatuses") or []:
        state = condition.get("state", {}).get("terminated")
        if state is not None:
            return int(state.get("exitCode", 1))
    return 1 if status.get("phase") == "Failed" else 0
