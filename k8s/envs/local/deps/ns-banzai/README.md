# namespace

## Description

This package provides a bare-bones `v1.Namespace` that you can build upon
and use in other packages.

## Usage

Clone this package:

```shell
kpt pkg get https://github.com/LCOGT/kpt-pkg-catalog/namespace ns
```

Customize `ns.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: example # <--- Change name
```

And then render to update resources:

```shell
kpt fn render
```

This package is also a Kustomization, so, it can also be referenced by other/parent
Kustomizations:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./ns/
```
