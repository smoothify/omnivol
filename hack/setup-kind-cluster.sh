#!/usr/bin/env bash
# hack/setup-kind-cluster.sh
#
# Bootstraps a local kind cluster suitable for testing omnivol.
# Installs:
#   - cert-manager (required by VolSync)
#   - VolSync
#   - openebs-lvm-localpv (backing StorageClass)
#   - omnivol CRDs
#
# Prerequisites (must be in PATH):
#   kind, kubectl, helm
#
# Usage:
#   ./hack/setup-kind-cluster.sh            # create cluster named "omnivol-dev"
#   CLUSTER_NAME=mytest ./hack/setup-kind-cluster.sh
#   KIND_DELETE_FIRST=1 ./hack/setup-kind-cluster.sh  # delete existing cluster first

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-omnivol-dev}"
KIND_DELETE_FIRST="${KIND_DELETE_FIRST:-0}"

CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.17.2}"
VOLSYNC_VERSION="${VOLSYNC_VERSION:-0.14.0}"
OPENEBS_VERSION="${OPENEBS_VERSION:-4.2.0}"

# ── helpers ──────────────────────────────────────────────────────────────────

info()  { echo "==> $*"; }
fatal() { echo "ERROR: $*" >&2; exit 1; }

require() {
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || fatal "'$cmd' not found in PATH"
    done
}

wait_for_rollout() {
    local ns="$1" kind="$2" name="$3"
    info "Waiting for $kind/$name in $ns …"
    kubectl rollout status "$kind/$name" -n "$ns" --timeout=300s
}

# ── pre-flight ────────────────────────────────────────────────────────────────

require kind kubectl helm

# ── cluster ──────────────────────────────────────────────────────────────────

if [[ "$KIND_DELETE_FIRST" == "1" ]]; then
    info "Deleting existing cluster '$CLUSTER_NAME' (KIND_DELETE_FIRST=1)"
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
fi

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    info "Cluster '$CLUSTER_NAME' already exists — skipping creation"
else
    info "Creating kind cluster '$CLUSTER_NAME'"
    cat <<EOF | kind create cluster --name "$CLUSTER_NAME" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
    extraMounts:
      # Expose host /dev so that openebs-lvm-localpv can access loop devices
      - hostPath: /dev
        containerPath: /dev
EOF
fi

kubectl cluster-info --context "kind-${CLUSTER_NAME}" >/dev/null

# ── cert-manager ─────────────────────────────────────────────────────────────

info "Installing cert-manager ${CERT_MANAGER_VERSION}"
helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
helm upgrade --install cert-manager jetstack/cert-manager \
    --namespace cert-manager \
    --create-namespace \
    --version "$CERT_MANAGER_VERSION" \
    --set crds.enabled=true \
    --wait \
    --timeout 5m

# ── VolSync ───────────────────────────────────────────────────────────────────

info "Installing VolSync ${VOLSYNC_VERSION}"
helm repo add backube https://backube.github.io/helm-charts/ --force-update >/dev/null
helm upgrade --install volsync backube/volsync \
    --namespace volsync-system \
    --create-namespace \
    --version "$VOLSYNC_VERSION" \
    --set manageCRDs=true \
    --wait \
    --timeout 5m

# ── openebs-lvm-localpv ───────────────────────────────────────────────────────
#
# For kind we use the hostpath CSI driver as a stand-in for openebs-lvm because
# openebs-lvm requires actual LVM on the host.  Set OPENEBS_LVM=1 if running on
# a node that genuinely has LVM configured.

if [[ "${OPENEBS_LVM:-0}" == "1" ]]; then
    info "Installing openebs-lvm-localpv ${OPENEBS_VERSION} (real LVM mode)"
    helm repo add openebs https://openebs.github.io/openebs --force-update >/dev/null
    helm upgrade --install openebs-lvm openebs/lvm-localpv \
        --namespace openebs \
        --create-namespace \
        --version "$OPENEBS_VERSION" \
        --wait \
        --timeout 5m

    kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-lvm
provisioner: local.csi.openebs.io
parameters:
  storage: "lvm"
  volgroup: "ubuntu-vg"
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
EOF
else
    info "Installing rancher/local-path-provisioner as openebs-lvm stand-in (kind mode)"
    kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml
    # Rename the default StorageClass name so omnivol BackupPolicy can reference it
    kubectl patch storageclass local-path -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}' 2>/dev/null || true
    kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-lvm
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
EOF
fi

# ── omnivol CRDs ─────────────────────────────────────────────────────────────

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

info "Applying omnivol CRDs"
kubectl apply -f "${REPO_ROOT}/config/crd/bases/"

# ── omnivol namespace ─────────────────────────────────────────────────────────

info "Creating omnivol-system namespace"
kubectl create namespace omnivol-system --dry-run=client -o yaml | kubectl apply -f -

# ── done ─────────────────────────────────────────────────────────────────────

info "Cluster '${CLUSTER_NAME}' is ready."
info ""
info "Next steps:"
info "  1. Create a BackupStore and BackupPolicy in the cluster"
info "     kubectl apply -f config/samples/"
info "  2. Run the controller locally:"
info "     go run ./cmd/ --kubeconfig \$HOME/.kube/config"
info "  3. Or build and load the image:"
info "     make docker-build IMG=omnivol:dev"
info "     kind load docker-image omnivol:dev --name ${CLUSTER_NAME}"
info "     make deploy IMG=omnivol:dev"
