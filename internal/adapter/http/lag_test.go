package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestLagHandler(t *testing.T) {
	t.Run("returns per-partition committed and latest offsets", func(t *testing.T) {
		handler := LagHandler{
			ListOffsetsForGroup: func(_ context.Context, group string) ([]GroupOffset, error) {
				if group != "workers" {
					t.Errorf("group = %q, want workers", group)
				}
				return []GroupOffset{{Topic: "events", Partition: 0, CommittedOffset: 4}, {Topic: "orders", Partition: 2, CommittedOffset: 8}}, nil
			},
			LatestOffset: func(topic string, partition int) (int64, error) {
				if topic == "events" && partition == 0 {
					return 9, nil
				}
				if topic == "orders" && partition == 2 {
					return 7, nil
				}
				t.Errorf("unexpected partition %s/%d", topic, partition)
				return 0, nil
			},
		}
		response := performLag(handler, http.MethodGet, "/lag?group=workers")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result []lagResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		want := []lagResponse{
			{Topic: "events", Partition: 0, CommittedOffset: 4, LatestOffset: 9, Lag: 5},
			{Topic: "orders", Partition: 2, CommittedOffset: 8, LatestOffset: 7, Lag: -1},
		}
		if !reflect.DeepEqual(result, want) {
			t.Errorf("response = %+v, want %+v", result, want)
		}
	})

	t.Run("returns empty array when group has no commits", func(t *testing.T) {
		handler := LagHandler{
			ListOffsetsForGroup: func(context.Context, string) ([]GroupOffset, error) { return []GroupOffset{}, nil },
			LatestOffset:        func(string, int) (int64, error) { t.Fatal("no partitions should be queried"); return 0, nil },
		}
		response := performLag(handler, http.MethodGet, "/lag?group=workers")
		var result []lagResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || result == nil || len(result) != 0 {
			t.Errorf("response = %d %+v, want empty array", response.Code, result)
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		response := performLag(LagHandler{}, http.MethodPost, "/lag?group=workers")
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("requires a group", func(t *testing.T) {
		response := performLag(LagHandler{}, http.MethodGet, "/lag")
		if response.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("requires dependencies", func(t *testing.T) {
		response := performLag(LagHandler{}, http.MethodGet, "/lag?group=workers")
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns offset list errors", func(t *testing.T) {
		handler := LagHandler{
			ListOffsetsForGroup: func(context.Context, string) ([]GroupOffset, error) { return nil, errors.New("database unavailable") },
			LatestOffset:        func(string, int) (int64, error) { return 0, nil },
		}
		response := performLag(handler, http.MethodGet, "/lag?group=workers")
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns latest offset errors", func(t *testing.T) {
		handler := LagHandler{
			ListOffsetsForGroup: func(context.Context, string) ([]GroupOffset, error) { return []GroupOffset{{Topic: "t"}}, nil },
			LatestOffset:        func(string, int) (int64, error) { return 0, errors.New("log unavailable") },
		}
		response := performLag(handler, http.MethodGet, "/lag?group=workers")
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})
}

func performLag(handler http.Handler, method, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
