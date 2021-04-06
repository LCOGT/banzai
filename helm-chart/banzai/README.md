# BANZAI Imaging Pipeline

This chart installs the following applications into a Kubernetes cluster.

- [BANZAI-Imaging](https://github.com/lcogt/banzai)

## Installing the Chart

To install the chart with the release name `banzai`:

```
$ helm install --name banzai banzai/ -f /path/to/values.yaml
```

## Uninstalling the Chart

To uninstall/delete the `banzai` deployment:

```
$ helm delete --purge banzai
```

## Configuration

You can customize the resources created by this chart by setting your desired
configuration values. Please read the chart and associated templates to find
all values which can be customized. Default values listed in the table represent 
what is present in the chart's default values file.

### image
Configuration parameters relating to the docker image that is being deployed.

Parameter | Description | Default
--- | --- | ---
`image.repository` | Docker image repository | `docker.lco.global/banzai`
`image.tag` | Docker image tag | **_Not set (required)_**
`image.pullPolicy` | [Kubernetes image pull policy](https://kubernetes.io/docs/concepts/configuration/overview/#container-images) | `IfNotPresent`

### ingester
Configuration parameters for the [OCS Ingester Library](https://github.com/observatorycontrolsystem/ingester)

#### Required values configuration
Parameter | Description | Default
--- | --- | ---
`ingester.apiRoot` | Ingester API root (API_ROOT environment variable)| **_Not set (required)_**
`ingester.s3Bucket` | Science Archive AWS S3 bucket (BUCKET environment variable) | **_Not set (required)_**
`ingester.noMetrics` | Do not submit metrics to an OpenTSDB server (see optional OpenTSDB configuration below) | `true`
`ingester.postProcessFiles` | Optionally submit files to fits queue | `false` 

#### Required secrets configuration
For sensitive keys, this helm chart will pull from the kubernetes secret `banzai-secrets` in your kubernetes namespace.

Environment Variable | Description | Secret | Key
--- | --- | --- | ---
`AUTH_TOKEN` | Authorization token for Science Archive. | `banzai-secrets` | `archive-auth-token`
`AWS_ACCESS_KEY_ID` | AWS access key ID for Archive S3 bucket. | `banzai-secrets` | `aws-access-key-id`
`AWS_SECRET_ACCESS_KEY` | AWS secret access key for Archive S3 bucket. | `banzai-secrets` | `aws-secret-access-key`

#### Optional OpenTSDB configuration
If you would like ingestion metrics posted to an [OpenTSDB](http://opentsdb.net/) server, set these values accordingly.

Parameter | Description | Default
--- | --- | ---
`ingester.ingesterProcessName` | A tag set with the collected metrics to identify where the metrics are coming from (INGESTER_PROCESS_NAME environment variable) | **_Not set_**
`ingester.opentsdbHostname` | OpenTSDB Host to send metrics to (OPENTSDB_HOSTNAME environment variable) | **_Not set_**
`ingester.opentsdbPort` | OpenTSDB erver port used to receive metrics (OPENTSDB_PORT environment variable) | **_Not set_**


### banzai
BANZAI-specific configuration

Parameter | Description | Default
--- | --- | ---
`banzai.astrometryServiceUrl` | URL for LCO GAIA-Astrometry service | **_Not set (required)_**
`banzai.configdbUrl` | URL for Configuration Database. | **_Not set (required)_**
`banzai.observationPortalUrl` | URL for LCO Observation Portal | **_Not set (required)_**
`banzai.calibrateProposalId` | Proposal ID for LCO Calibration requests | **_Not set (required)_**
`banzai.banzaiWorkerLogLevel` | Log level for BANZAI worker processes | **_Not set (required)_**
`fitsBroker` | Hostname for FITS files queue | **_Not set (required)_**
`fitsExchange` | Exchange name for FITS files queue | **_Not set (required)_**
`queueName` | BANZAI file queue name (fanout from fitsExchange) | **_Not set (required)_**

#### Optional separate raw and processed archive sources
During development, it is sometimes necessary to pull raw data from a "production" science archive, and push/pull processed
data to/from a "development" science archive running independently in a separate namespace.

By default, BANZAI will push/pull to and from the same science archive. If separate archive sources are desired, a raw data
source may be specified. Please note that the processed data source is assumed always to be the source configured in the 
science archive section.

Parameter | Description | Default
--- | --- | ---
`banzai.useDifferentArchiveSources` | Use different archives for raw and processed data. If set to true, optional raw data source configuration must be specified. | `false`
`banzai.rawDataApiRoot` | API root URL for raw data source | **_Not set_**

The authorization token for the raw data source is pulled from the `banzai-secrets` kubernetes secret

Environment Variable | Description | Secret | Key
--- | --- | --- | ---
`RAW_DATA_AUTH_TOKEN` | Authorization token for raw data source | `banzai-secrets` | `raw-data-auth-token`

### PostgreSQL Database Configuration

This helm chart configures BANZAI to use a Postgresql backend. It can be configured to use an existing database, or spin up its
own.

#### Existing DB Configuration

When using an existing database (`.Values.useDockerizedDatabase = false`), set the following values:

Parameter | Description | Default
--- | --- | ---
`postgresql.hostname` | Hostname for postgresql server | **_Not set_**
`postgresql.postgresqlUsername` | Username for postgresql DB | **_Not set_**
`postgresql.postgresqlDatabase` | Database name for BANZAI DB | **_Not set_**

Additionally, specify the DB password associated with the username you set via a kubernetes secret

Environment Variable | Description | Secret | Key
--- | --- | --- | ---
`DB_PASSWORD` | Password for BANZAI DB | `banzai-secrets` | `postgresql-password`


#### Using a Dockerized database

See values-dev for a working example of using a dockerized database.

### RabbitMQ Configuration

#### Existing RabbitMQ Configuration

When using an existing RabbitMQ server (`.Values.useDockerizedRabbitMQ = false`), set the following values:

Parameter | Description | Default
--- | --- | ---
`rabbitmq.hostname` | Hostname for RabbitMQ server | **_Not set_**
`rabbitmq.rabbitmq.username` | Username for RabbitMQ instance | **_Not set_**
`rabbitmq.vhost` | RabbitMQ Vhost | **_Not set_**

Environment Variable | Description | Secret | Key
--- | --- | --- | ---
`RABBITMQ_PASSWORD` | Password for RabbitMQ instance | `banzai-secrets` | `rabbitmq-password`

#### Using a Dockerized RabbitMQ

See values-dev for a working example of using a dockerized RabbitMQ

## Appendix: Secrets

Below is an example YAML file, which specifies a Kubernetes secret, `banzai-secrets`.
See Kubernetes secrets documentation [here](https://kubernetes.io/docs/concepts/configuration/secret/) 

```yaml
apiVersion: v1
data:
  archive-auth-token:
  # raw-data-auth-token key only needed if .Values.useDifferentArchiveSources is true
  raw-data-auth-token:
  aws-access-key-id:
  aws-secret-access-key:
  postgresql-password:
  rabbitmq-password:
kind: Secret
metadata:
  name: banzai-secrets
  namespace: 
type: Opaque
```

Data stored in a Kubernetes secret is usually stored as a base64-encoded string. To properly store a secret value, 
first encode it as base64.

If my `postgresql-password` is `secret_password`, I will need to encode it before creating the secret.

```bash
echo -n secret_password | base64
c2VjcmV0X3Bhc3N3b3Jk
```

Enter the base64-encoded string into the proper place in the above template.

```yaml
data:
...
  postgresql-password: c2VjcmV0X3Bhc3N3b3Jk
...
```

Finally, be sure to set the kubernetes namespace name inside the `metadata` section. The secret will exist only in
that namespace. This is the namespace you intend to deploy the application.

Once all required values have been set, create the secret in your namespace by:

```bash
kubectl apply -f banzai-secrets.yaml
```
