package main

import "html/template"

var uiTemplate = template.Must(template.New("ui").Parse(`<!DOCTYPE html>
<html>
<head>
<style>
body {
	font-family: sans-serif;
}
table {
	border-collapse: collapse;
}
.progress {
	border: 1px solid black;
	background: white;
	height: 20px;
	width: 100px;
	padding: 0;
	margin: 0;
}
.complete {
	height: 20px;
	background: blue;
	padding: 0;
	margin: 0;
}
</style>
<body>
<table>
<p><a href="/">Refresh</a></p>
{{range .}}
<tr>
	<td>
	{{if .State}}
		&nbsp;
	{{else}}
		<form method="POST" action="/">
		<input type="hidden" name="start" value="{{.URL}}">
		<input type="submit" value="Start Now">
		</form>
	{{end}}
	</td>
	<td><div class="progress"><div class="complete" style="width: {{.PercentDone}}%"></div></div></td>
	<td>{{.Name}}</td>
	<td>{{.Size}}</td>
	<td>{{.State}}</td>
</tr>
{{end}}
</table>
</body>
</html>
`))
