# deployment

## Description

This package provides a bare-bones [`apps/v1.Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
that you can build upon and use in other packages.

## Usage

Clone this package:

```shell
kpt pkg get https://github.com/LCOGT/kpt-pkg-catalog/deployment deploy-myapp
```

Customize `deploy.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
  # Name will be used as the value for the `app.kubernetes.io/component`
  # selector label and updated automatically by `kpt fn render`.
  # So no need to set those manually.
  name: test
```

And then render to update resources:

```shell
kpt fn render
```

This package is also a Kustomization, so, it can also be referenced by other
Kustomizations:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./deploy-myapp/
```
