package httpadapter

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Rwatana/miniKafka/internal/broker"
)

type topicReaderStub struct {
	partitions []broker.PartitionInfo
	listError  error
	offsets    map[broker.PartitionInfo]int64
	offsetErr  error
}

func (reader topicReaderStub) ListPartitions() ([]broker.PartitionInfo, error) {
	return reader.partitions, reader.listError
}

func (reader topicReaderStub) LatestOffset(topic string, partition int) (int64, error) {
	if reader.offsetErr != nil {
		return 0, reader.offsetErr
	}
	return reader.offsets[broker.PartitionInfo{Topic: topic, Partition: partition}], nil
}

func TestTopicsHandler(t *testing.T) {
	t.Run("returns topic partitions and latest offsets", func(t *testing.T) {
		reader := topicReaderStub{
			partitions: []broker.PartitionInfo{{Topic: "orders", Partition: 0}, {Topic: "orders", Partition: 2}},
			offsets: map[broker.PartitionInfo]int64{
				{Topic: "orders", Partition: 0}: 3,
				{Topic: "orders", Partition: 2}: 8,
			},
		}
		response := performTopics(TopicsHandler{Broker: reader}, http.MethodGet)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
		}
		var result []topicResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		want := []topicResponse{{Topic: "orders", Partition: 0, LatestOffset: 3}, {Topic: "orders", Partition: 2, LatestOffset: 8}}
		if !reflect.DeepEqual(result, want) {
			t.Errorf("response = %+v, want %+v", result, want)
		}
	})

	t.Run("returns an empty array when no partitions exist", func(t *testing.T) {
		response := performTopics(TopicsHandler{Broker: topicReaderStub{}}, http.MethodGet)
		var result []topicResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || result == nil || len(result) != 0 {
			t.Errorf("response = %d %+v, want empty array", response.Code, result)
		}
	})

	t.Run("rejects non-GET methods", func(t *testing.T) {
		response := performTopics(TopicsHandler{Broker: topicReaderStub{}}, http.MethodPost)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("returns partition listing errors", func(t *testing.T) {
		reader := topicReaderStub{listError: errors.New("list failed")}
		response := performTopics(TopicsHandler{Broker: reader}, http.MethodGet)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("returns latest offset errors", func(t *testing.T) {
		reader := topicReaderStub{
			partitions: []broker.PartitionInfo{{Topic: "orders", Partition: 0}},
			offsetErr:  errors.New("offset failed"),
		}
		response := performTopics(TopicsHandler{Broker: reader}, http.MethodGet)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
		}
	})
}

func performTopics(handler http.Handler, method string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/topics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
