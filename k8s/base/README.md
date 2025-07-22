# banzai

This package provides a base Kustomization for deploying Banzai.

## Usage

In another `kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  # Pull in the base resources (use `kustomize localize` to pre-fetch)
  - http://github.com/LCOGT/banzai//k8s/base?ref=main

# Add a prefix to every resource name for this instance
namePrefix: my-banzai-

labels:
  # Add a label identifiying this instance
  - pairs:
      app.kubernetes.io/instance: my-banzai
    includeSelectors: true
    includeTemplates: true

configMapGenerator:
  # Set environment variables needed by Banzai
  - name: env
    behavior: merge
    literals:
      - CONFIGDB_URL=https://...
      - ASTROMETRY_SERVICE_URL=https://...
```

## Secrets

Although sensitive environment variables can be provided using the `env` configMapGenerator,
they may also be provided in the following optional `Secret` resources:

```yaml

secretGenerator:
  # Generic catch-all
  - name: env
    envs:
      - ./secrets.env

  # Secrets can also be split by category (e.g. for making SealedSecret rotation easier)
  # Database envs
  - name: db-env
    envs:
      - ./db.env

  # Rabbitmq envs
  - name: rabbitmq-env
    envs:
      - ./rabbitmq.env

  # Archive API envs
  - name: archive-api-env
    envs:
      - ./archive-api.env
```
