#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${KIND_CLUSTER_NAME:-trpc-platform-smoke-$$}"
cluster_context="kind-${cluster_name}"
cluster_created=false

for executable in docker helm istioctl kind kubectl kubectl-argo-rollouts; do
  if ! command -v "${executable}" >/dev/null 2>&1; then
    echo "missing required executable: ${executable}" >&2
    exit 2
  fi
done

cleanup() {
  if [[ "${cluster_created}" == true && "${KEEP_KUBERNETES_SMOKE_CLUSTER:-0}" != "1" ]]; then
    kind delete cluster --name "${cluster_name}"
  fi
}

terminate() {
  local status="$1"
  trap - EXIT TERM INT
  cleanup
  exit "${status}"
}

trap cleanup EXIT
trap 'terminate 143' TERM
trap 'terminate 130' INT

kube() {
  kubectl --context "${cluster_context}" "$@"
}

wait_for_application() {
  local application="$1"
  local sync=""
  local health=""
  local operation=""
  for _ in $(seq 1 180); do
    sync="$(kube get application "${application}" -n argocd -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
    health="$(kube get application "${application}" -n argocd -o jsonpath='{.status.health.status}' 2>/dev/null || true)"
    operation="$(kube get application "${application}" -n argocd -o jsonpath='{.status.operationState.phase}' 2>/dev/null || true)"
    if [[ "${sync}" == "Synced" && "${operation}" == "Succeeded" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Application ${application} did not sync (sync=${sync}, health=${health}, operation=${operation})" >&2
  kubectl --context "${cluster_context}" get applications -n argocd -o wide >&2
  kubectl --context "${cluster_context}" describe application "${application}" -n argocd >&2
  kube get jobs,pods -n platform-smoke -o wide >&2 || true
  kube describe jobs -n platform-smoke \
    -l app.kubernetes.io/component=database-migration >&2 || true
  kube logs -n platform-smoke \
    -l app.kubernetes.io/component=database-migration --all-containers=true >&2 || true
  return 1
}

wait_for_failed_analysis() {
  for _ in $(seq 1 90); do
    if kube get analysisruns -n platform-smoke \
      -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -q '^Failed$'; then
      return 0
    fi
    sleep 2
  done
  echo "The deliberately unhealthy canary did not fail analysis" >&2
  kube get analysisruns -n platform-smoke -o wide >&2
  return 1
}

create_probe() {
  local namespace="$1"
  local name="$2"
  local service_account="$3"
  local network_profile="${4:-}"
  local labels="trpc-agent-platform.io/test-probe=true"
  local overrides
  if [[ -n "${network_profile}" ]]; then
    labels="${labels},trpc-agent-platform.io/network-profile=${network_profile}"
  fi
  overrides="$(printf '%s' \
    '{"apiVersion":"v1","spec":{' \
    "\"serviceAccountName\":\"${service_account}\"," \
    '"automountServiceAccountToken":false,"containers":[{' \
    "\"name\":\"${name}\",\"image\":\"${probe_image}\",\"imagePullPolicy\":\"Never\"," \
    '"command":["sleep","600"],"resources":{"requests":{"cpu":"10m","memory":"16Mi"},' \
    '"limits":{"cpu":"100m","memory":"64Mi"}}}]}}')"
  kube run "${name}" -n "${namespace}" \
    --image="${probe_image}" \
    --image-pull-policy=Never \
    --labels="${labels}" \
    --restart=Never \
    --overrides="${overrides}"
  kube wait "pod/${name}" -n "${namespace}" --for=condition=Ready --timeout=90s
}

expect_http_result() {
  local namespace="$1"
  local pod="$2"
  local url="$3"
  local expected="$4"
  local header="${5:-}"
  kube exec -i -n "${namespace}" "${pod}" -- \
    /app/.venv/bin/python - "${url}" "${expected}" "${header}" <<'PY'
import sys
import urllib.error
import urllib.request

url, expected, header = sys.argv[1:]
request = urllib.request.Request(url)
if header:
    name, value = header.split("=", 1)
    request.add_header(name, value)

try:
    with urllib.request.urlopen(request, timeout=10) as response:
        actual = str(response.status)
except urllib.error.HTTPError as exc:
    actual = str(exc.code)
except (TimeoutError, urllib.error.URLError, ConnectionError):
    actual = "blocked"

if expected == "blocked":
    if actual.startswith("2"):
        raise SystemExit(f"expected {url} to be blocked, got HTTP {actual}")
elif actual != expected:
    raise SystemExit(f"expected {url} to return {expected}, got {actual}")
PY
}

cd "${repository_root}"
kind create cluster --name "${cluster_name}" \
  --config tests/deployment/fixtures/kind-config.yaml \
  --wait 120s
cluster_created=true

kube apply --server-side -f \
  https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.6.0/experimental-install.yaml
istioctl install --context "${cluster_context}" --set profile=ambient \
  --set values.pilot.env.ENABLE_INGRESS_WAYPOINT_ROUTING=true \
  --skip-confirmation
kube rollout status deployment/istiod -n istio-system --timeout=240s
kube rollout status daemonset/istio-cni-node -n istio-system --timeout=240s
kube rollout status daemonset/ztunnel -n istio-system --timeout=240s

docker build --quiet -t local/trpc-agent-platform:smoke -f Dockerfile.admin-api .
docker build --quiet -t local/trpc-agent-web-console:smoke web-console
docker build --quiet -t local/trpc-agent-git:smoke \
  -f tests/deployment/fixtures/git-server.Dockerfile .
docker build --quiet -t local/trpc-agent-postgres:smoke \
  -f tests/deployment/fixtures/postgres-smoke.Dockerfile .
platform_digest="$(docker image inspect --format '{{.Id}}' local/trpc-agent-platform:smoke)"
web_digest="$(docker image inspect --format '{{.Id}}' local/trpc-agent-web-console:smoke)"
kind load docker-image --name "${cluster_name}" \
  local/trpc-agent-platform:smoke \
  local/trpc-agent-web-console:smoke \
  local/trpc-agent-git:smoke \
  local/trpc-agent-postgres:smoke
for node in $(kind get nodes --name "${cluster_name}"); do
  docker exec "${node}" ctr -n k8s.io images tag \
    docker.io/local/trpc-agent-platform:smoke \
    "docker.io/local/trpc-agent-platform@${platform_digest}"
  docker exec "${node}" ctr -n k8s.io images tag \
    docker.io/local/trpc-agent-web-console:smoke \
    "docker.io/local/trpc-agent-web-console@${web_digest}"
done

kube create namespace argo-rollouts
kube label namespace argo-rollouts istio.io/dataplane-mode=ambient
kube apply --server-side --force-conflicts -n argo-rollouts -f \
  https://github.com/argoproj/argo-rollouts/releases/download/v1.10.0/install.yaml
kube rollout status deployment/argo-rollouts -n argo-rollouts --timeout=180s

kube apply -f \
  https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml
kube patch deployment metrics-server -n kube-system --type=json -p \
  '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kube rollout status deployment/metrics-server -n kube-system --timeout=180s

kube create namespace argocd
kube apply --server-side --force-conflicts -n argocd -f \
  https://raw.githubusercontent.com/argoproj/argo-cd/v3.5.2/manifests/core-install.yaml
kube set env deployment/argocd-applicationset-controller -n argocd \
  ARGOCD_APPLICATIONSET_CONTROLLER_ENABLE_PROGRESSIVE_SYNCS=true
kube rollout status deployment/argocd-applicationset-controller -n argocd --timeout=240s
kube rollout status deployment/argocd-repo-server -n argocd --timeout=240s
kube rollout status statefulset/argocd-application-controller -n argocd --timeout=240s

kube apply -f tests/deployment/fixtures/git-server.yaml
if ! kube rollout status deployment/git-server -n smoke-system --timeout=120s; then
  kube describe deployment/git-server -n smoke-system >&2
  kube get pods -n smoke-system -o wide >&2
  kube logs deployment/git-server -n smoke-system >&2
  exit 1
fi
kube apply -f tests/deployment/fixtures/postgres-smoke.yaml
if ! kube rollout status deployment/smoke-postgres -n platform-smoke --timeout=120s; then
  kube describe deployment/smoke-postgres -n platform-smoke >&2
  kube get pods -n platform-smoke -o wide >&2
  kube logs deployment/smoke-postgres -n platform-smoke >&2
  exit 1
fi
sed \
  -e "s/PLATFORM_IMAGE_DIGEST/${platform_digest}/g" \
  -e "s/WEB_IMAGE_DIGEST/${web_digest}/g" \
  tests/deployment/fixtures/argocd-smoke.yaml | kube apply -f -

for unit in admin-api web-console agent-gateway channel-gateway agent-worker job-worker; do
  wait_for_application "smoke-${unit}"
  workload_kind=deployment
  if [[ "${unit}" != admin-api && "${unit}" != web-console ]]; then
    workload_kind=rollout
  fi
  workload_image="$(kube get "${workload_kind}" -n platform-smoke \
    -l "app.kubernetes.io/component=${unit}" \
    -o jsonpath='{.items[0].spec.template.spec.containers[0].image}')"
  if [[ "${workload_image}" != *@sha256:* ]]; then
    echo "${unit} did not deploy an immutable digest: ${workload_image}" >&2
    exit 1
  fi
done

kube rollout status deployment -n platform-smoke \
  -l app.kubernetes.io/component=admin-api --timeout=180s
kube rollout status deployment -n platform-smoke \
  -l app.kubernetes.io/component=web-console --timeout=180s
for unit in agent-gateway channel-gateway agent-worker job-worker; do
  unit_rollout="$(kube get rollouts -n platform-smoke \
    -l "app.kubernetes.io/component=${unit}" -o jsonpath='{.items[0].metadata.name}')"
  kubectl-argo-rollouts --context "${cluster_context}" status "${unit_rollout}" \
    -n platform-smoke --timeout 180s
done

probe_image="docker.io/local/trpc-agent-platform@${platform_digest}"
security_uids_before="$(kube get \
  peerauthentication/platform-strict-mtls \
  gateway/platform-waypoint \
  networkpolicy/platform-workloads-default-deny \
  networkpolicy/platform-waypoint \
  -n platform-smoke -o jsonpath='{range .items[*]}{.kind}{"/"}{.metadata.name}{"="}{.metadata.uid}{"\n"}{end}' | sort)"
admin_deployment="$(kube get deployment -n platform-smoke \
  -l app.kubernetes.io/component=admin-api -o jsonpath='{.items[0].metadata.name}')"
kube set image "deployment/${admin_deployment}" -n platform-smoke \
  admin-api=docker.io/invalid/image:smoke
kube patch application smoke-admin-api -n argocd --type=merge \
  -p '{"operation":{"sync":{"prune":true}}}'
for _ in $(seq 1 90); do
  reconciled_image="$(kube get "deployment/${admin_deployment}" -n platform-smoke \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
  if [[ "${reconciled_image}" == "${probe_image}" ]]; then
    break
  fi
  sleep 2
done
if [[ "${reconciled_image}" != "docker.io/local/trpc-agent-platform@${platform_digest}" ]]; then
  echo "Argo CD did not reconcile the Admin API drift: ${reconciled_image}" >&2
  exit 1
fi
security_uids_after="$(kube get \
  peerauthentication/platform-strict-mtls \
  gateway/platform-waypoint \
  networkpolicy/platform-workloads-default-deny \
  networkpolicy/platform-waypoint \
  -n platform-smoke -o jsonpath='{range .items[*]}{.kind}{"/"}{.metadata.name}{"="}{.metadata.uid}{"\n"}{end}' | sort)"
if [[ "${security_uids_before}" != "${security_uids_after}" ]]; then
  echo "Persistent zero-trust resources were recreated during reconciliation" >&2
  diff <(printf '%s\n' "${security_uids_before}") <(printf '%s\n' "${security_uids_after}") >&2 || true
  exit 1
fi
istioctl analyze --context "${cluster_context}" -n platform-smoke --failure-threshold Error
kube wait gateway/platform-waypoint -n platform-smoke \
  --for=condition=Programmed --timeout=180s

kube create serviceaccount unauthorized-caller -n platform-smoke
kube create namespace outside-mesh
create_probe platform-smoke web-console-caller web-console web-console
create_probe platform-smoke unauthorized-caller unauthorized-caller web-console
create_probe argo-rollouts rollout-caller argo-rollouts
create_probe outside-mesh unmeshed-caller default

expect_http_result platform-smoke web-console-caller \
  http://admin-api:8000/api/v1/health 200
expect_http_result platform-smoke web-console-caller \
  http://admin-api:8000/api/docs 403
expect_http_result platform-smoke web-console-caller \
  http://admin-api:8000/api/v1/health 403 x-tenant-id=forged
expect_http_result platform-smoke unauthorized-caller \
  http://admin-api:8000/api/v1/health 403
expect_http_result outside-mesh unmeshed-caller \
  http://admin-api.platform-smoke.svc:8000/api/v1/health blocked
expect_http_result argo-rollouts rollout-caller \
  http://agent-gateway-stable.platform-smoke.svc:8000/health/ready 200

kube delete applicationset trpc-agent-platform-smoke -n argocd --cascade=orphan
kube patch application smoke-agent-gateway -n argocd --type=merge \
  -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":false}}}}'

rollout_name="$(kube get rollouts -n platform-smoke \
  -l app.kubernetes.io/component=agent-gateway \
  -o jsonpath='{.items[0].metadata.name}')"
kube patch rollout "${rollout_name}" -n platform-smoke --type=json -p \
  '[{"op":"add","path":"/spec/template/metadata/annotations","value":{"smoke-revision":"unhealthy"}},{"op":"replace","path":"/spec/strategy/canary/steps/2/analysis/args/1/value","value":"wrong-service"}]'
wait_for_failed_analysis

expect_http_result argo-rollouts rollout-caller \
  http://agent-gateway-stable.platform-smoke.svc:8000/health/ready 200

kube patch rollout "${rollout_name}" -n platform-smoke --type=json -p \
  '[{"op":"replace","path":"/spec/strategy/canary/steps/2/analysis/args/1/value","value":"agent-gateway"}]'
kubectl-argo-rollouts --context "${cluster_context}" undo "${rollout_name}" -n platform-smoke
kubectl-argo-rollouts --context "${cluster_context}" status "${rollout_name}" \
  -n platform-smoke --timeout 180s

expect_http_result argo-rollouts rollout-caller \
  http://agent-gateway-stable.platform-smoke.svc:8000/health/ready 200

echo "Kubernetes smoke passed: Ambient mTLS and identity/path/tenant denials held while Argo Rollouts safely rejected and rolled back an unhealthy canary."
