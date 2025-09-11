# helm-chart

## Description

This package will let you render a Helm chart.

## Usage

Clone this package:

```shell
kpt pkg get https://github.com/LCOGT/kpt-pkg-catalog/helm-chart example-helm
```

Define Helm charts in `charts.yaml`:

```yaml
apiVersion: kpt.dev/v1
kind: RenderHelmChart
metadata:
  name: postgresql # Change this to a short name describing the charts
  annotations:
    config.kubernetes.io/local-config: "true"
# See https://catalog.kpt.dev/render-helm-chart/v0.2/
helmCharts:
  - chartArgs:
    repo: oci://registry-1.docker.io/bitnamicharts
    name: postgresql
    version: 12.12.10
   templateOptions:
    apiVersions:
      - "1.23.17"
    releaseName: postgresql
    namespace: example-ns
    includeCRDs: true
    skipTests: true
    values:
      valuesInline:
        # Chart values go here
        architecture: standalone
        primary.persistence.size: 2Gi
        auth:
          database: example
          username: example
          password: example
```

Then run:

```shell
kpt fn render --allow-network
```

This will template out the Chart(s) and place them in `rendered.yaml`.

This package is also a Kustomization that includes the Chart(s) output, so
you can use it from other Kustomizations:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./example-helm/
```

Note `helmCharts[].templateOptions.namespace` does not actually cause a
`v1.Namespace` to be emmited. That must be created seperately, if it does not
already exist on the cluster.
Consider using https://github.com/LCOGT/kpt-pkg-catalog/tree/main/namespace to do that.
