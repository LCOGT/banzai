{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "banzai.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "banzai.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "banzai.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "banzai.labels" -}}
app.kubernetes.io/name: {{ include "banzai-nres.name" . }}
helm.sh/chart: {{ include "banzai-nres.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}


{{/*
Generate the PostgreSQL DB hostname
*/}}
{{- define "banzai.dbhost" -}}
{{- if .Values.postgresql.fullnameOverride -}}
{{- .Values.postgresql.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if .Values.useDockerizedDatabase -}}
{{- printf "%s-postgresql" .Release.Name -}}
{{- else -}}
{{- required "`postgresql.hostname` must be set when `useDockerizedDatabase` is `false`" .Values.postgresql.hostname -}}
{{- end -}}
{{- end -}}

{{- define "banzai.rabbitmq" -}}
{{- if .Values.rabbitmq.fullnameOverride -}}
{{- .Values.rabbitmq.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if .Values.useDockerizedRabbitMQ -}}
{{- printf "%s-rabbitmq" .Release.Name -}}
{{- else -}}
{{- required "`rabbitmq.hostname` must be set when `useDockerizedRabbitMQ` is `false`" .Values.rabbitmq.hostname -}}
{{- end -}}
{{- end -}}

{{/*
Define shared environment variables
*/}}
{{- define "banzai.Env" -}}
- name: DB_HOST
  value: {{ include "banzai.dbhost" . | quote }}
- name: RABBITMQ_HOST
  value: {{ include "banzai.rabbitmq" . | quote }}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: postgresqlPassword
- name: DB_USER
  value: {{ .Values.postgresql.postgresqlUsername | quote }}
- name: DB_NAME
  value: {{ .Values.postgresql.postgresqlDatabase | quote }}
- name: DB_ADDRESS
  value: postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST)/$(DB_NAME)
- name: RABBITMQ_PASSWORD
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: rabbitmq-password
- name: TASK_HOST
  value: amqp://{{ .Values.rabbitmq.rabbitmq.username }}:$(RABBITMQ_PASSWORD)@$(RABBITMQ_HOST)/{{ .Values.rabbitmq.vhost }}
- name: RETRY_DELAY
  value: "600000"
- name: CALIBRATE_PROPOSAL_ID
  value: {{ .Values.CALIBRATE_PROPOSAL_ID | quote }}
- name: OBSERVATION_PORTAL_URL
  value: {{ .Values.OBSERVATION_PORTAL_URL | quote }}
- name: API_ROOT
  value:  {{ .Values.API_ROOT | quote }}
- name: AUTH_TOKEN
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: AUTH_TOKEN
- name: BUCKET
  value: {{ .Values.BUCKET | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: AWS_ACCESS_KEY_ID
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: AWS_SECRET_ACCESS_KEY
- name: OPENTSDB_HOSTNAME
  value: {{ .Values.OPENTSDB_HOSTNAME | quote }}
- name: BOSUN_HOSTNAME
  value: {{ .Values.BOSUN_HOSTNAME | quote }}
- name: FITS_BROKER
  value: {{ .Values.FITS_BROKER | quote }}
- name: FITS_EXCHANGE
  value: {{ .Values.FITS_EXCHANGE | quote }}
- name: INGESTER_PROCESS_NAME
  value: {{ .Values.INGESTER_PROCESS_NAME | quote }}
- name: POSTPROCESS_FILES
  value: "False"
- name: BANZAI_WORKER_LOGLEVEL
  value: {{ .Values.BANZAI_WORKER_LOGLEVEL | quote }}
- name: RAW_DATA_FRAME_URL
  value: {{ .Values.RAW_DATA_FRAME_URL | quote }}
- name: RAW_DATA_AUTH_TOKEN
  valueFrom:
    secretKeyRef:
      name: banzai-secrets
      key: RAW_DATA_AUTH_TOKEN
{{ if .Values.NO_METRICS  }}
- name: OPENTSDB_PYTHON_METRICS_TEST_MODE
  value: "1"
{{- end -}}

{{- end -}}
