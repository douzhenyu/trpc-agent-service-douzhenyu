#!/usr/bin/env bash
set -euo pipefail

destination="${1:-}"
if [[ -z "${destination}" ]]; then
  echo "usage: $0 <destination-directory>" >&2
  exit 2
fi

helm_version="v4.2.4"
kind_version="v0.33.0"
rollouts_version="v1.10.0"
istio_version="1.31.0"
platform="$(uname -s)-$(uname -m)"

case "${platform}" in
  Darwin-arm64)
    release_platform="darwin-arm64"
    istio_platform="osx-arm64"
    helm_sha="d747eb4e28bd2727173d15b759fa0a17822291ec09db7ced3d55af290a3661a2"
    kind_sha="0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f"
    rollouts_sha="0046896141a09e15913d0c4c2651516fb46b233f5595a5d2e462fa9a0d9d1e69"
    istio_sha="a0c4d35320f930e5254de119512f0761f65a76fb614973281bf9435e24306d66"
    ;;
  Darwin-x86_64)
    release_platform="darwin-amd64"
    istio_platform="osx-amd64"
    helm_sha="6c163d687ca03c3b5c01928e53bbbcf9518278f47ce7a2f249a5a08e8bdaa2bc"
    kind_sha="5a99f26f57246dc9319dd294803313197a0f34d33c525b3ea8b655db5916ece0"
    rollouts_sha="1a41cdf72c45eb0bbe6fc6fcaa9529b8372044be33f1cea827487ad8d0bad395"
    istio_sha="8bbc6e93757e342ca90aa7b14be1d4f77272afe65db2ea8545e62875619a0005"
    ;;
  Linux-aarch64)
    release_platform="linux-arm64"
    istio_platform="linux-arm64"
    helm_sha="564de2191b881e9f71b5606b25345821ea1682f06ab90499d3ab22b530176da1"
    kind_sha="20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2"
    rollouts_sha="2d73e61091084769d16191f21fc686b9c2054892eb93d59046660e8c876a6865"
    istio_sha="c2367fb837d8fd10ed9a742c1927d8207b06fdc5271255a7288eafd8f2a85d42"
    ;;
  Linux-x86_64)
    release_platform="linux-amd64"
    istio_platform="linux-amd64"
    helm_sha="c306b46f719b0a4da32d0f78ee21bf90ce8d602f15b22ab753f0674d1670a7f3"
    kind_sha="aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d"
    rollouts_sha="57a464e80c3e716076c9760e1d15ff06b853e3bcab3e22e30f4dba8a3e9f29b2"
    istio_sha="ef3f4368122c9008d344b7c824e563dae6fec9078eb2b90e87836063887d15a8"
    ;;
  *)
    echo "unsupported platform: ${platform}" >&2
    exit 2
    ;;
esac

install_tmp="$(mktemp -d)"
trap 'rm -rf "${install_tmp}"' EXIT
mkdir -p "${destination}"

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 "$1" | cut -d ' ' -f 1
  fi
}

verify() {
  local file="$1"
  local expected="$2"
  local actual
  actual="$(checksum "${file}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "checksum mismatch for ${file}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
}

helm_archive="${install_tmp}/helm.tar.gz"
curl -fsSL "https://get.helm.sh/helm-${helm_version}-${release_platform}.tar.gz" -o "${helm_archive}"
verify "${helm_archive}" "${helm_sha}"
tar -xzf "${helm_archive}" -C "${install_tmp}"
install -m 0755 "${install_tmp}/${release_platform}/helm" "${destination}/helm"

curl -fsSL \
  "https://kind.sigs.k8s.io/dl/${kind_version}/kind-${release_platform}" \
  -o "${install_tmp}/kind"
verify "${install_tmp}/kind" "${kind_sha}"
install -m 0755 "${install_tmp}/kind" "${destination}/kind"

curl -fsSL \
  "https://github.com/argoproj/argo-rollouts/releases/download/${rollouts_version}/kubectl-argo-rollouts-${release_platform}" \
  -o "${install_tmp}/kubectl-argo-rollouts"
verify "${install_tmp}/kubectl-argo-rollouts" "${rollouts_sha}"
install -m 0755 "${install_tmp}/kubectl-argo-rollouts" "${destination}/kubectl-argo-rollouts"

istio_archive="${install_tmp}/istioctl.tar.gz"
curl -fsSL \
  "https://github.com/istio/istio/releases/download/${istio_version}/istioctl-${istio_version}-${istio_platform}.tar.gz" \
  -o "${istio_archive}"
verify "${istio_archive}" "${istio_sha}"
tar -xzf "${istio_archive}" -C "${install_tmp}"
install -m 0755 "${install_tmp}/istioctl" "${destination}/istioctl"

echo "Installed Helm ${helm_version}, Kind ${kind_version}, Argo Rollouts ${rollouts_version}, and Istio ${istio_version} in ${destination}"
