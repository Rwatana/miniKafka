package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestConsumeHandler(t *testing.T) {
	t.Run("uses the group's committed offset and defaults", func(t *testing.T) {
		store := &broker.Broker{DataDir: t.TempDir()}
		if _, err := store.Produce("orders", 0, nil, "first"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Produce("orders", 0, nil, "second"); err != nil {
			t.Fatal(err)
		}
		lookupCalls := 0
		handler := ConsumeHandler{
			Broker: store,
			GetCommittedOffset: func(_ context.Context, group, topic string, partition int) (int64, error) {
				lookupCalls++
				if group != "orders-workers" || topic != "orders" || partition != 0 {
					t.Errorf("lookup arguments = %q, %q, %d", group, topic, partition)
				}
				return 1, nil
			},
		}
		response := performConsume(handler, http.MethodGet, "/consume?topic=orders&group=orders-workers")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result consumeResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if lookupCalls != 1 {
			t.Errorf("lookup calls = %d, want 1", lookupCalls)
		}
		if result.Topic != "orders" || result.Partition != 0 || result.FromOffset != 1 || len(result.Messages) != 1 || result.Messages[0].Value != "second" {
			t.Errorf("response = %+v, want one record from offset 1", result)
		}
	})

	t.Run("uses an explicit offset and supplied bounds", func(t *testing.T) {
		store := &broker.Broker{DataDir: t.TempDir()}
		if _, err := store.Produce("orders", 3, nil, "selected"); err != nil {
			t.Fatal(err)
		}
		handler := ConsumeHandler{
			Broker: store,
			GetCommittedOffset: func(context.Context, string, string, int) (int64, error) {
				t.Fatal("explicit offset must skip committed lookup")
				return 0, nil
			},
		}
		response := performConsume(handler, http.MethodGet, "/consume?topic=orders&group=g&partition=3&max=1&offset=0")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result consumeResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result.Topic != "orders" || result.Partition != 3 || result.FromOffset != 0 || len(result.Messages) != 1 || result.Messages[0].Value != "selected" {
			t.Errorf("response = %+v, want selected record from orders/3", result)
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		response := performConsume(ConsumeHandler{}, http.MethodPost, "/consume")
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("requires topic and group", func(t *testing.T) {
		for _, target := range []string{"/consume?group=g", "/consume?topic=t"} {
			response := performConsume(ConsumeHandler{}, http.MethodGet, target)
			if response.Code != http.StatusBadRequest {
				t.Errorf("target %s status = %d, want %d", target, response.Code, http.StatusBadRequest)
			}
		}
	})

	t.Run("rejects malformed and negative numeric parameters", func(t *testing.T) {
		for _, target := range []string{
			"/consume?topic=t&group=g&partition=bad",
			"/consume?topic=t&group=g&partition=-1",
			"/consume?topic=t&group=g&max=bad",
			"/consume?topic=t&group=g&max=-1",
			"/consume?topic=t&group=g&offset=bad",
			"/consume?topic=t&group=g&offset=-1",
		} {
			response := performConsume(ConsumeHandler{}, http.MethodGet, target)
			if response.Code != http.StatusBadRequest {
				t.Errorf("target %s status = %d, want %d", target, response.Code, http.StatusBadRequest)
			}
		}
	})

	t.Run("requires committed offset lookup when offset is omitted", func(t *testing.T) {
		response := performConsume(ConsumeHandler{Broker: &broker.Broker{DataDir: t.TempDir()}}, http.MethodGet, "/consume?topic=t&group=g")
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns committed offset lookup errors", func(t *testing.T) {
		handler := ConsumeHandler{
			Broker: &broker.Broker{DataDir: t.TempDir()},
			GetCommittedOffset: func(context.Context, string, string, int) (int64, error) {
				return 0, errors.New("database unavailable")
			},
		}
		response := performConsume(handler, http.MethodGet, "/consume?topic=t&group=g")
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "database unavailable") {
			t.Errorf("response = %d %s, want database error", response.Code, response.Body)
		}
	})

	t.Run("returns broker errors", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(dataDir, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		handler := ConsumeHandler{
			Broker: &broker.Broker{DataDir: dataDir},
			GetCommittedOffset: func(context.Context, string, string, int) (int64, error) {
				return 0, nil
			},
		}
		response := performConsume(handler, http.MethodGet, "/consume?topic=t&group=g")
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns an empty messages array for no matching records", func(t *testing.T) {
		handler := ConsumeHandler{
			Broker:             &broker.Broker{DataDir: t.TempDir()},
			GetCommittedOffset: func(context.Context, string, string, int) (int64, error) { return 0, nil },
		}
		response := performConsume(handler, http.MethodGet, "/consume?topic=t&group=g&max=0")
		var result struct {
			Messages []storage.Record `json:"messages"`
		}
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || result.Messages == nil || len(result.Messages) != 0 {
			t.Errorf("response = %d %+v, want empty messages array", response.Code, result)
		}
	})
}

func performConsume(handler http.Handler, method, target string) *httptest.ResponseRecorder {
	parsed, err := url.ParseRequestURI(target)
	if err != nil {
		panic(err)
	}
	request := httptest.NewRequest(method, parsed.String(), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
