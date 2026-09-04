"""Sandbox policy: the hardening contract every sandbox pod must satisfy.

The policy is the single source of truth for the sandbox threat model: gVisor
runtime, non-root, read-only root filesystem, zero capabilities, no service
account token, no networking and bounded CPU, memory, time and output. Any
attempt to weaken a hard requirement fails construction, so no caller can
configure a silently degraded sandbox (spec 条目23/禁项8).
"""

from __future__ import annotations

import base64
import hashlib
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.sandbox.errors import SandboxError


class SandboxPolicy(BaseModel):
    """Immutable sandbox pod hardening contract."""

    model_config = ConfigDict(frozen=True)

    image: str = Field(min_length=1)
    runtime_class_name: str = Field(default="gvisor", pattern=r"^[a-z][a-z0-9-]{0,62}$")
    run_as_non_root: bool = True
    run_as_user: int = Field(default=65532, gt=0)
    read_only_root_filesystem: bool = True
    allow_privilege_escalation: bool = False
    capabilities: tuple[str, ...] = ("ALL",)
    automount_service_account_token: bool = False
    network_disabled: bool = True
    cpu_limit: str = Field(default="1", pattern=r"^\d+m$|^\d+(\.\d+)?$")
    memory_limit: str = Field(default="512Mi", pattern=r"^\d+(Mi|Gi|M|G)$")
    execution_timeout_seconds: int = Field(default=30, ge=1, le=600)
    max_output_bytes: int = Field(default=65536, ge=1024)

    def model_post_init(self, _context: Any) -> None:
        if self.runtime_class_name != "gvisor":
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: runtime must be gvisor")
        if not self.run_as_non_root or self.run_as_user == 0:
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: must run as non-root")
        if not self.read_only_root_filesystem:
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: root filesystem must be read-only")
        if self.allow_privilege_escalation:
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: privilege escalation must be denied")
        if self.capabilities != ("ALL",):
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: all capabilities must be dropped")
        if self.automount_service_account_token:
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: service account token must be off")
        if not self.network_disabled:
            raise SandboxError("SANDBOX_POLICY_TOO_WEAK: networking must be disabled")


class SandboxArtifact:
    """A named byte payload exchanged with the sandbox, pinned by digest."""

    __slots__ = ("name", "content", "sha256")

    def __init__(self, name: str, content: bytes, sha256: str) -> None:
        self.name = name
        self.content = content
        self.sha256 = sha256

    @classmethod
    def from_bytes(cls, name: str, content: bytes) -> SandboxArtifact:
        return cls(name=name, content=content, sha256=hashlib.sha256(content).hexdigest())


def sandbox_pod_spec(policy: SandboxPolicy, *, execution_id: str) -> dict[str, Any]:
    """The hardened Kubernetes Pod manifest for one sandbox execution."""

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"sandbox-{execution_id[:32]}",
            "labels": {
                "platform.trpc/sandbox": "true",
                "platform.trpc/execution-id": execution_id[:64],
            },
        },
        "spec": {
            "runtimeClassName": policy.runtime_class_name,
            "automountServiceAccountToken": False,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": policy.execution_timeout_seconds,
            "containers": [
                {
                    "name": "sandbox",
                    "image": policy.image,
                    "imagePullPolicy": "IfNotPresent",
                    "securityContext": {
                        "runAsNonRoot": policy.run_as_non_root,
                        "runAsUser": policy.run_as_user,
                        "readOnlyRootFilesystem": policy.read_only_root_filesystem,
                        "allowPrivilegeEscalation": policy.allow_privilege_escalation,
                        "capabilities": {"drop": list(policy.capabilities)},
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "resources": {
                        "limits": {
                            "cpu": policy.cpu_limit,
                            "memory": policy.memory_limit,
                        },
                        "requests": {
                            "cpu": policy.cpu_limit,
                            "memory": policy.memory_limit,
                        },
                    },
                    "env": [{"name": "SANDBOX", "value": "1"}],
                }
            ],
        },
    }


def sandbox_network_policy(policy: SandboxPolicy, *, namespace: str) -> dict[str, Any]:
    """Default-deny egress NetworkPolicy for sandbox pods."""

    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "sandbox-deny-egress",
            "namespace": namespace,
        },
        "spec": {
            "podSelector": {"matchLabels": {"platform.trpc/sandbox": "true"}},
            "policyTypes": ["Egress"],
            "egress": [],
        },
    }


def encode_artifact(artifact: SandboxArtifact) -> str:
    """Base64 payload used to hand an input artifact to the sandbox."""

    return base64.b64encode(artifact.content).decode("ascii")
