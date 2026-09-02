{{/*
Expand the chart name.
*/}}
{{- define "fq.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fq.fullname" -}}
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
Chart name and version label.
*/}}
{{- define "fq.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "fq.labels" -}}
helm.sh/chart: {{ include "fq.chart" . }}
{{ include "fq.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "fq.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fq.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name.
*/}}
{{- define "fq.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fq.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Secret name for fq tokens.
*/}}
{{- define "fq.authSecretName" -}}
{{- default (printf "%s-auth" (include "fq.fullname" .)) .Values.auth.existingSecret -}}
{{- end -}}

{{/*
Container image reference.
*/}}
{{- define "fq.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{/*
How fq should read Kubernetes Secret-backed tokens.
*/}}
{{- define "fq.authTokenSource" -}}
{{- $source := default "file" .Values.auth.tokenSource -}}
{{- if not (or (eq $source "file") (eq $source "env")) -}}
{{- fail "auth.tokenSource must be one of: file, env" -}}
{{- end -}}
{{- $source -}}
{{- end -}}
