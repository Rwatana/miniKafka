package httpadapter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardHandler(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	DashboardHandler{}.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want HTML", contentType)
	}
	for _, marker := range []string{"MiniKafka", "getJSON('/topics')", "'/lag?group='", "latestOffset", "committedOffset"} {
		if !strings.Contains(response.Body.String(), marker) {
			t.Errorf("dashboard page does not contain %q", marker)
		}
	}
}
