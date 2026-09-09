package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var assets embed.FS

// handler serves the embedded dashboard without a separate frontend build
func Handler() http.Handler {
	staticFiles, _ := fs.Sub(assets, "static")
	return http.StripPrefix("/dashboard/", http.FileServer(http.FS(staticFiles)))
}
