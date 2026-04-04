---
description: Build and deploy omnivol controller to a cluster for dev/testing
---

# Deploy Dev Build

Builds the controller image locally, pushes to GHCR with the `dev` tag, applies CRDs, and restarts the deployment on the target cluster.

**Prerequisites:**
- Docker (or OrbStack) with `buildx` available
- Authenticated to GHCR (`docker login ghcr.io`)
- `kubectl` context set to the target cluster

## Steps

// turbo-all

1. Regenerate manifests and deepcopy from types/markers:
```bash
make manifests generate
```

2. Verify the code compiles:
```bash
go build ./...
```

3. Build the linux/arm64 image and push to GHCR:
```bash
docker buildx build --platform=linux/arm64 --push --tag ghcr.io/smoothify/omnivol:dev -f Dockerfile .
```

> **Note:** adjust `--platform` if deploying to an amd64 cluster (use `linux/amd64`), or use `linux/amd64,linux/arm64` for multi-arch.

4. Apply CRDs to the cluster:
```bash
kubectl apply -f config/crd/bases/
```

5. Restart the controller deployment to pull the new image:
```bash
kubectl rollout restart deployment/omnivol -n omnivol-system
```

6. Wait for rollout to complete:
```bash
kubectl rollout status deployment/omnivol -n omnivol-system --timeout=120s
```

7. Verify the pod is Running:
```bash
kubectl get pods -n omnivol-system
```

8. Tail logs to check for errors:
```bash
kubectl logs -n omnivol-system deployment/omnivol -c manager -f --tail=50
```
