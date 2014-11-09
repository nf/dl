package main

import "html/template"

var uiTemplate = template.Must(template.New("ui").Parse(`<!DOCTYPE html>
<html>
<body>
{{range .}}
<p>{{.URL}} {{.Received}}/{{.Size}} {{.State}}</p>
{{end}}
</body>
</html>
`))
