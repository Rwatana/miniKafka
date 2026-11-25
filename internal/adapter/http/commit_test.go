package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCommitHandler(t *testing.T) {
	t.Run("defaults partition and commits zero offset", func(t *testing.T) {
		calls := 0
		handler := CommitHandler{CommitOffset: func(_ context.Context, group, topic string, partition int, offset int64) error {
			calls++
			if group != "orders-workers" || topic != "orders" || partition != 0 || offset != 0 {
				t.Errorf("commit arguments = %q, %q, %d, %d", group, topic, partition, offset)
			}
			return nil
		}}
		response := performCommit(handler, http.MethodPost, `{"group":"orders-workers","topic":"orders","offset":0}`)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result commitResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if calls != 1 || result != (commitResponse{Committed: true, Group: "orders-workers", Topic: "orders", Partition: 0, Offset: 0}) {
			t.Errorf("calls = %d, response = %+v", calls, result)
		}
	})

	t.Run("uses supplied partition and offset", func(t *testing.T) {
		handler := CommitHandler{CommitOffset: func(_ context.Context, group, topic string, partition int, offset int64) error {
			if group != "workers" || topic != "events" || partition != 4 || offset != 21 {
				t.Errorf("commit arguments = %q, %q, %d, %d", group, topic, partition, offset)
			}
			return nil
		}}
		response := performCommit(handler, http.MethodPost, `{"group":"workers","topic":"events","partition":4,"offset":21}`)
		if response.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", response.Code, http.StatusOK)
		}
	})

	t.Run("rejects non-POST methods", func(t *testing.T) {
		response := performCommit(CommitHandler{}, http.MethodGet, `{}`)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("rejects malformed JSON", func(t *testing.T) {
		response := performCommit(CommitHandler{}, http.MethodPost, "{")
		if response.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("requires group topic and offset", func(t *testing.T) {
		for _, body := range []string{
			`{"topic":"t","offset":1}`,
			`{"group":"g","offset":1}`,
			`{"group":"g","topic":"t"}`,
			`{"group":" ","topic":"t","offset":1}`,
			`{"group":"g","topic":" ","offset":1}`,
		} {
			response := performCommit(CommitHandler{}, http.MethodPost, body)
			if response.Code != http.StatusBadRequest {
				t.Errorf("body %s status = %d, want %d", body, response.Code, http.StatusBadRequest)
			}
		}
	})

	t.Run("rejects malformed or negative numeric values", func(t *testing.T) {
		for _, body := range []string{
			`{"group":"g","topic":"t","partition":"bad","offset":1}`,
			`{"group":"g","topic":"t","partition":-1,"offset":1}`,
			`{"group":"g","topic":"t","offset":"bad"}`,
			`{"group":"g","topic":"t","offset":-1}`,
		} {
			response := performCommit(CommitHandler{}, http.MethodPost, body)
			if response.Code != http.StatusBadRequest {
				t.Errorf("body %s status = %d, want %d", body, response.Code, http.StatusBadRequest)
			}
		}
	})

	t.Run("requires a commit dependency", func(t *testing.T) {
		response := performCommit(CommitHandler{}, http.MethodPost, `{"group":"g","topic":"t","offset":1}`)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns persistence errors", func(t *testing.T) {
		handler := CommitHandler{CommitOffset: func(context.Context, string, string, int, int64) error {
			return errors.New("database unavailable")
		}}
		response := performCommit(handler, http.MethodPost, `{"group":"g","topic":"t","offset":1}`)
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "database unavailable") {
			t.Errorf("response = %d %s, want database error", response.Code, response.Body)
		}
	})
}

func performCommit(handler http.Handler, method, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/commit", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
