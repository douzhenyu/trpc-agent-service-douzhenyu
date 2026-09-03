{{- define "trpc-agent-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "trpc-agent-platform.image" -}}
{{- if .component.image.digest -}}
{{- printf "%s@%s" .component.image.repository .component.image.digest -}}
{{- else if and .root.Values.global.allowMutableTags .component.image.tag -}}
{{- printf "%s:%s" .component.image.repository .component.image.tag -}}
{{- else -}}
{{- fail "every production image requires a digest; mutable tags must be explicitly enabled" -}}
{{- end -}}
{{- end }}

{{- define "trpc-agent-platform.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "trpc-agent-platform.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
