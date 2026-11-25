package httpadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rwatana/miniKafka/internal/broker"
)

func TestProduceHandler(t *testing.T) {
	t.Run("defaults partition to zero", func(t *testing.T) {
		handler := ProduceHandler{Broker: &broker.Broker{DataDir: t.TempDir()}}
		response := performProduce(handler, `{"topic":"orders","value":"created"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result produceResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result != (produceResponse{Topic: "orders", Partition: 0, Offset: 0}) {
			t.Errorf("response = %+v, want orders/0 offset 0", result)
		}
	})

	t.Run("uses supplied partition and key", func(t *testing.T) {
		dataDir := t.TempDir()
		broker := &broker.Broker{DataDir: dataDir}
		handler := ProduceHandler{Broker: broker}
		response := performProduce(handler, `{"topic":"orders","partition":3,"key":"customer-42","value":"created"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result produceResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result != (produceResponse{Topic: "orders", Partition: 3, Offset: 0}) {
			t.Errorf("response = %+v, want orders/3 offset 0", result)
		}
		contents, err := os.ReadFile(filepath.Join(dataDir, "orders__3.log"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(contents), `"key":"customer-42"`) {
			t.Errorf("partition log = %s, want supplied key", contents)
		}
	})

	t.Run("rejects malformed JSON", func(t *testing.T) {
		response := performProduce(ProduceHandler{Broker: &broker.Broker{DataDir: t.TempDir()}}, "{")
		if response.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("requires topic and value", func(t *testing.T) {
		for _, body := range []string{`{"value":"v"}`, `{"topic":"orders"}`} {
			response := performProduce(ProduceHandler{Broker: &broker.Broker{DataDir: t.TempDir()}}, body)
			if response.Code != http.StatusBadRequest {
				t.Errorf("body %s status = %d, want %d", body, response.Code, http.StatusBadRequest)
			}
		}
	})

	t.Run("rejects a negative partition", func(t *testing.T) {
		response := performProduce(ProduceHandler{Broker: &broker.Broker{DataDir: t.TempDir()}}, `{"topic":"orders","partition":-1,"value":"v"}`)
		if response.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("returns broker errors", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "file-not-directory")
		if err := os.WriteFile(dataDir, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		response := performProduce(ProduceHandler{Broker: &broker.Broker{DataDir: dataDir}}, `{"topic":"orders","value":"v"}`)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})
}

func performProduce(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/produce", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
