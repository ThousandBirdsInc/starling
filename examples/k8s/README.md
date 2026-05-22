# Starling k8s example (kind)

A minimal Kubernetes example: a busybox static server, built with
`docker_build`, deployed to a local cluster, with `live_update` and a named URL.

## Prerequisites

- Docker running
- A **local** Kubernetes cluster. [kind](https://kind.sigs.k8s.io/) is simplest:

  ```bash
  brew install kind            # or: go install sigs.k8s.io/kind@latest
  kind create cluster --name starling
  ```

  (k3d, minikube, or Docker Desktop's Kubernetes work too. Don't point Starling
  at a shared/production cluster.)

## Run

```bash
cd examples/k8s
starling up
```

Starling will:

1. build the `starling-demo` image (via the Docker API),
2. load it into the kind cluster — with kind you may need
   `kind load docker-image starling-demo --name starling` once,
3. `kubectl apply` the Deployment + Service,
4. watch the pod's status and stream its logs,
5. expose it at a named URL through the shared proxy.

Open the dashboard with `starling` (k9s-style TUI) to watch builds/pods/logs.

## Live update

Edit `app/index.html`. Because the `docker_build` declares a `live_update`
`sync`, Starling copies the file straight into the running pod (`kubectl cp`)
instead of rebuilding the image and redeploying — refresh to see the change.
