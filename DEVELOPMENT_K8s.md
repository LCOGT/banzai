# Development with Kubernetes

## Shell

Always enter the development shell before doing anything else. This will make
sure you are using the same version of tools as everyone else to avoid any
system discrepancies.

Install [Nix](https://github.com/LCOGT/public-wiki/wiki/Install-Nix) if you have
not already.

If you have [direnv](https://github.com/LCOGT/public-wiki/wiki/Install-direnv)
installed, the shell will automatically activate and deactive anytime you change
directories. You may have to grant permissions initially with:

```sh
direnv allow
```

Otherwise, you can manually enter the shell with:

```sh
./develop.sh
```

## Development Cluster

Spin up a development cluster with:

```sh
devenv-k8s-cluster-up
```

This script is provided by the development shell and wraps some calls to setup
a basic Kind cluster.

You can also use another mechanisim to setup a development cluster if you'd like.


## Skaffold

Deploy application dependencies (RabbitMQ & Postgres):

```sh
skaffold -m banzai-deps run
```

Configure secret environment variables (e.g. `AUTH_TOKEN`) needed by Banzai in:

```file
./k8s/envs/local/secrets.env
```

Envrionment variables that should be checked into VCS are set in:

```file
./k8s/envs/local/settings.env
```

Deploy Banzai:

```sh
skaffold -m banzai run
```

This will deploy Banzai resources to the `banzai` namespace.

Initialize the database if you have not already. This is done by CronJobs in the
base Kustomization but are disabled in the local development overlay.
To run them manually:

```sh
kubectl -n banzai create job db-instrument-update-init --from=cronjob/db-instrument-update
kubectl -n banzai create job db-bpm-update-init --from=cronjob/db-bpm-update
kubectl -n banzai wait --for=condition=complete --timeout 10m job/db-instrument-update-init job/db-bpm-update-init
```

This is also available as `skaffold-banzai-init-db` as a shortcut.

### Integration Tests

Integration tests can be run in the `debug` Pod.


#### Super Bias Creation

```sh
kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_bias
# or
skaffold-banzai-e2e-master-bias
```

#### Super Dark Creation

```sh
kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_dark
# or
skaffold-banzai-e2e-master-dark
```

#### Super Flat Creation

```sh
kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_flat
# or
skaffold-banzai-e2e-master-flat
```

#### Science Frame Creation

```sh
kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m science_files
# or
skaffold-banzai-e2e-master-science-files
```
