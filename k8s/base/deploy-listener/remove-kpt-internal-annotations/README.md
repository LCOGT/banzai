# remove-kpt-internal-annotations

## Description

This package provides a [Kustomize `Component`](https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/1802-kustomize-components)
that can be used to remove `internal.kpt.dev/upstream-identifier` annotations from all rendered KRM objects.

## Usage

Clone this package:

```shell
kpt pkg get https://github.com/LCOGT/kpt-pkg-catalog/remove-kpt-internal-annotations
```

And then reference it from another Kustomization:


```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

components:
  - ./remove-kpt-internal-annotations/
```
