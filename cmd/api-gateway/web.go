package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/*
var webAppFiles embed.FS

func registerWebApp(mux *http.ServeMux) {
	webFS, err := fs.Sub(webAppFiles, "web")
	if err != nil {
		panic(err)
	}

	mux.Handle("GET /app/", http.StripPrefix("/app/", http.FileServer(http.FS(webFS))))
	mux.HandleFunc("GET /app", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/app/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/app/", http.StatusTemporaryRedirect)
	})
}
