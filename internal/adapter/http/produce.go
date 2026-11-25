package httpadapter

import (
	"encoding/json"
	"net/http"

	"github.com/Rwatana/miniKafka/internal/broker"
)

type produceRequest struct {
	Topic     string  `json:"topic"`
	Partition *int    `json:"partition"`
	Key       *string `json:"key"`
	Value     *string `json:"value"`
}

type produceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// ProduceHandler serves the HTTP endpoint for producing one topic-partition record.
type ProduceHandler struct {
	Broker *broker.Broker
}

// ServeHTTP validates one produce request and delegates it to the broker.
func (handler ProduceHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	var input produceRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "invalid JSON request"})
		return
	}
	if input.Topic == "" || input.Value == nil {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "topic and value are required"})
		return
	}

	partition := 0
	if input.Partition != nil {
		partition = *input.Partition
	}
	if partition < 0 {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "partition must be non-negative"})
		return
	}

	offset, err := handler.Broker.Produce(input.Topic, partition, input.Key, *input.Value)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(response).Encode(produceResponse{Topic: input.Topic, Partition: partition, Offset: offset})
}
