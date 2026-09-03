#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${KIND_CLUSTER_NAME:-trpc-platform-smoke-$$}"
cluster_context="kind-${cluster_name}"
cluster_created=false

for executable in docker helm kind kubectl kubectl-argo-rollouts; do
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
trap cleanup EXIT

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

cd "${repository_root}"
kind create cluster --name "${cluster_name}" \
  --config tests/deployment/fixtures/kind-config.yaml \
  --wait 120s
cluster_created=true

docker build --quiet -t local/trpc-agent-platform:smoke -f Dockerfile.admin-api .
docker build --quiet -t local/trpc-agent-web-console:smoke web-console
docker build --quiet -t local/trpc-agent-git:smoke \
  -f tests/deployment/fixtures/git-server.Dockerfile .
platform_digest="$(docker image inspect --format '{{.Id}}' local/trpc-agent-platform:smoke)"
web_digest="$(docker image inspect --format '{{.Id}}' local/trpc-agent-web-console:smoke)"
kind load docker-image --name "${cluster_name}" \
  local/trpc-agent-platform:smoke \
  local/trpc-agent-web-console:smoke \
  local/trpc-agent-git:smoke
for node in $(kind get nodes --name "${cluster_name}"); do
  docker exec "${node}" ctr -n k8s.io images tag \
    docker.io/local/trpc-agent-platform:smoke \
    "docker.io/local/trpc-agent-platform@${platform_digest}"
  docker exec "${node}" ctr -n k8s.io images tag \
    docker.io/local/trpc-agent-web-console:smoke \
    "docker.io/local/trpc-agent-web-console@${web_digest}"
done

kube create namespace argo-rollouts
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
probe_overrides="$(printf '%s' \
  '{"apiVersion":"v1","spec":{"containers":[{"name":"public-boundary-probe",' \
  "\"image\":\"${probe_image}\",\"imagePullPolicy\":\"Never\"," \
  '"command":["sleep","600"],"resources":{"requests":{"cpu":"10m","memory":"16Mi"},' \
  '"limits":{"cpu":"100m","memory":"64Mi"}}}]}}')"
kube run public-boundary-probe -n platform-smoke \
  --image="${probe_image}" \
  --image-pull-policy=Never \
  --labels=app.kubernetes.io/name=trpc-agent-platform \
  --restart=Never \
  --overrides="${probe_overrides}"
kube wait pod/public-boundary-probe -n platform-smoke --for=condition=Ready --timeout=90s

kube exec -n platform-smoke public-boundary-probe -- \
  /app/.venv/bin/python -c \
  "import urllib.request; urllib.request.urlopen('http://admin-api:8000/api/v1/health')"
kube exec -n platform-smoke public-boundary-probe -- \
  /app/.venv/bin/python -c \
  "import urllib.request; urllib.request.urlopen('http://web-console:8080/api/v1/health')"
for unit in agent-gateway channel-gateway agent-worker job-worker; do
  kube exec -n platform-smoke public-boundary-probe -- \
    /app/.venv/bin/python -c \
    "import urllib.request; urllib.request.urlopen('http://${unit}-stable:8000/health/ready')"
done

kube delete applicationset trpc-agent-platform-smoke -n argocd --cascade=orphan
kube patch application smoke-agent-gateway -n argocd --type=merge \
  -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":false}}}}'

rollout_name="$(kube get rollouts -n platform-smoke \
  -l app.kubernetes.io/component=agent-gateway \
  -o jsonpath='{.items[0].metadata.name}')"
kube patch rollout "${rollout_name}" -n platform-smoke --type=json -p \
  '[{"op":"add","path":"/spec/template/metadata/annotations","value":{"smoke-revision":"unhealthy"}},{"op":"replace","path":"/spec/strategy/canary/steps/2/analysis/args/1/value","value":"wrong-service"}]'
wait_for_failed_analysis

kube exec -n platform-smoke public-boundary-probe -- \
  /app/.venv/bin/python -c \
  "import urllib.request; urllib.request.urlopen('http://agent-gateway-stable:8000/health/ready')"

kube patch rollout "${rollout_name}" -n platform-smoke --type=json -p \
  '[{"op":"replace","path":"/spec/strategy/canary/steps/2/analysis/args/1/value","value":"agent-gateway"}]'
kubectl-argo-rollouts --context "${cluster_context}" undo "${rollout_name}" -n platform-smoke
kubectl-argo-rollouts --context "${cluster_context}" status "${rollout_name}" \
  -n platform-smoke --timeout 180s

kube exec -n platform-smoke public-boundary-probe -- \
  /app/.venv/bin/python -c \
  "import urllib.request; urllib.request.urlopen('http://agent-gateway-stable:8000/health/ready')"

echo "Kubernetes smoke passed: Argo CD synced six units and Argo Rollouts rejected and rolled back an unhealthy canary."
