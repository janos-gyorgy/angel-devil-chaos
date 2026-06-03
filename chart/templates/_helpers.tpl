{{/*
Common labels for every devilangel-owned resource. Useful for bulk queries
("kubectl get all -A -l app.kubernetes.io/part-of=devilangel") and for the
delete-test in the plan's verification section.
*/}}
{{- define "devilangel.labels" -}}
app.kubernetes.io/part-of: devilangel
app.kubernetes.io/managed-by: helm
{{- end -}}
