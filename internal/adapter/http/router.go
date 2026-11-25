package httpadapter

import "net/http"

// NewRouter registers the broker API and dashboard handlers on their reference paths.
func NewRouter(produce, consume, commit, topics, lag, dashboard http.Handler) http.Handler {
	router := http.NewServeMux()
	router.Handle("POST /produce", produce)
	router.Handle("GET /consume", consume)
	router.Handle("POST /commit", commit)
	router.Handle("GET /topics", topics)
	router.Handle("GET /lag", lag)
	router.Handle("GET /{$}", dashboard)
	return router
}
