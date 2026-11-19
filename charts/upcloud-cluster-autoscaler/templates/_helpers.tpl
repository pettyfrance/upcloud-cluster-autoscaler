{{- define "uca.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "uca.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "uca.selectorLabels" -}}
app.kubernetes.io/name: {{ include "uca.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "uca.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "uca.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Name of the Secret holding the UpCloud token, after checking exactly one source is set. */}}
{{- define "uca.secretName" -}}
{{- if and .Values.upcloud.token .Values.upcloud.existingSecret }}
{{- fail "Set only one of upcloud.token and upcloud.existingSecret, not both." }}
{{- else if .Values.upcloud.existingSecret }}
{{- .Values.upcloud.existingSecret }}
{{- else if .Values.upcloud.token }}
{{- include "uca.fullname" . }}
{{- else }}
{{- fail "An UpCloud API token is required: set upcloud.existingSecret (recommended) or upcloud.token." }}
{{- end }}
{{- end }}

{{- define "uca.secretKey" -}}
{{- if .Values.upcloud.existingSecret }}
{{- .Values.upcloud.existingSecretKey }}
{{- else }}
{{- "token" }}
{{- end }}
{{- end }}
