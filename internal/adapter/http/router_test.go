package httpadapter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewRouter(t *testing.T) {
	markerHandler := func(marker string) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			_, _ = response.Write([]byte(marker))
		})
	}
	router := NewRouter(
		markerHandler("produce"), markerHandler("consume"), markerHandler("commit"),
		markerHandler("topics"), markerHandler("lag"), markerHandler("dashboard"),
	)
	for _, test := range []struct {
		method string
		path   string
		want   string
	}{
		{method: http.MethodPost, path: "/produce", want: "produce"},
		{method: http.MethodGet, path: "/consume", want: "consume"},
		{method: http.MethodPost, path: "/commit", want: "commit"},
		{method: http.MethodGet, path: "/topics", want: "topics"},
		{method: http.MethodGet, path: "/lag", want: "lag"},
		{method: http.MethodGet, path: "/", want: "dashboard"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != test.want {
				t.Errorf("response = %d %q, want 200 %q", response.Code, response.Body.String(), test.want)
			}
		})
	}

	t.Run("wrong method is rejected", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/produce", nil)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("unknown path is not handled by the dashboard", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/unknown", nil)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", response.Code, http.StatusNotFound)
		}
	})
}
