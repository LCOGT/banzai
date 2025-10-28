# cronjob

## Description

This package provides a bare-bones [`batch/v1.Cronjob`](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
that you can build upon and use in other packages.

## Usage

Clone this package:

```shell
kpt pkg get https://github.com/LCOGT/kpt-pkg-catalog/cronjob cronjob-myname
```

Customize `cronjob.yaml`:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: test
spec:
  schedule: "@hourly"
  jobTemplate:
    spec:
      backoffLimit: 3
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: default
              image: "busybox:latest"
```

And then render to update resources:

```shell
kpt fn render
```

This package is also a Kustomization, so it can also be referenced by other
Kustomizations:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./cronjob-myname/
```
