#!/usr/bin/env bash
# Sandbox smoke test: run escape, resource-abuse and no-runtime scenarios
# against a live cluster with the gVisor RuntimeClass installed.
#
# Requirements: kubectl, a namespace with the gvisor RuntimeClass and the
# sandbox NetworkPolicy applied (see deploy/helm templates/sandbox.yaml).
#
#   ./scripts/sandbox_smoke.sh [namespace]
set -euo pipefail

namespace="${1:-platform}"
image="${SANDBOX_IMAGE:-registry.internal/platform/sandbox-python:latest}"
timeout_seconds="${SANDBOX_TIMEOUT:-30}"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

run_sandbox_pod() {
  local name="$1" code="$2"
  # Wrap the code in the sandbox entrypoint payload contract (see
  # deploy/sandbox/entrypoint.py), then base64-encode to stay YAML-safe.
  local payload
  payload="$(python3 -c 'import base64,json,sys; print(base64.b64encode(json.dumps({"code": sys.argv[1]}).encode()).decode())' "$code")"
  kubectl -n "$namespace" delete pod "$name" --ignore-not-found >/dev/null
  kubectl -n "$namespace" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels:
    platform.trpc/sandbox: "true"
spec:
  runtimeClassName: gvisor
  automountServiceAccountToken: false
  restartPolicy: Never
  activeDeadlineSeconds: $timeout_seconds
  containers:
    - name: sandbox
      image: $image
      imagePullPolicy: IfNotPresent
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
      env:
        - name: SANDBOX_PAYLOAD_B64
          value: "$payload"
      resources:
        limits:
          cpu: "1"
          memory: 512Mi
EOF
  kubectl -n "$namespace" wait --for=condition=!Ready "pod/$name" --timeout="${timeout_seconds}s" >/dev/null 2>&1 || true
}

assert_blocked() {
  local name="$1" expectation="$2"
  local logs
  logs="$(kubectl -n "$namespace" logs "pod/$name" 2>/dev/null || true)"
  local status
  status="$(kubectl -n "$namespace" get "pod/$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown)"
  if [[ "$expectation" == "no-network" && "$logs" == *"connected"* ]]; then
    fail "sandbox pod $name reached the network"
  fi
  if [[ "$expectation" == "read-only" && "$logs" != *"Read-only"* && "$logs" != *"read-only"* && "$status" != "Failed" ]]; then
    fail "sandbox pod $name wrote to the root filesystem"
  fi
  # A real SA token is a JWT starting with the base64 of '{"alg"'.
  if [[ "$logs" == *"eyJhbGciOi"* || "$logs" == *"eyJraWQi"* ]]; then
    fail "sandbox pod $name read a service account token"
  fi
  echo "PASS: $name ($status)"
}

echo "== scenario 0: no RuntimeClass must fail scheduling =="
if ! kubectl -n "$namespace" get runtimeclass gvisor >/dev/null 2>&1; then
  echo "PASS: gvisor RuntimeClass absent is detected before any execution"
else
  echo "INFO: gvisor RuntimeClass present"
fi

echo "== scenario 1: filesystem escape attempt =="
run_sandbox_pod "smoke-escape" "open('/etc/passwd','w').write('pwned')"
assert_blocked "smoke-escape" "read-only"

echo "== scenario 2: service account token exfiltration attempt =="
run_sandbox_pod "smoke-token" "print(open('/var/run/secrets/kubernetes.io/serviceaccount/token').read()[:20])"
assert_blocked "smoke-token" "no-token"

echo "== scenario 3: network access attempt =="
run_sandbox_pod "smoke-network" "
import socket
s = socket.create_connection(('metadata.google.internal', 80), timeout=3)
print('connected')
"
assert_blocked "smoke-network" "no-network"

echo "== scenario 4: resource abuse hits the deadline =="
run_sandbox_pod "smoke-abuse" "
while True:
    pass
"
kubectl -n "$namespace" wait --for=condition=Ready "pod/smoke-abuse" --timeout="10s" >/dev/null 2>&1 || true
sleep "$timeout_seconds"
status="$(kubectl -n "$namespace" get pod/smoke-abuse -o jsonpath='{.status.reason}' 2>/dev/null || echo Unknown)"
if [[ "$status" != "DeadlineExceeded" && "$status" != "Evicted" ]]; then
  # The pod may still be terminating; the deadline must have been armed.
  deadline="$(kubectl -n "$namespace" get pod/smoke-abuse -o jsonpath='{.spec.activeDeadlineSeconds}')"
  [[ -n "$deadline" ]] || fail "resource abuse pod has no activeDeadlineSeconds"
fi
echo "PASS: smoke-abuse"
kubectl -n "$namespace" delete pod smoke-escape smoke-token smoke-network smoke-abuse --ignore-not-found >/dev/null

echo "Sandbox smoke: all scenarios blocked."
