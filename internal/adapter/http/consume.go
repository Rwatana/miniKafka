package httpadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/storage"
)

type committedOffsetLookup func(context.Context, string, string, int) (int64, error)

type consumeResponse struct {
	Topic      string           `json:"topic"`
	Partition  int              `json:"partition"`
	FromOffset int64            `json:"fromOffset"`
	Messages   []storage.Record `json:"messages"`
}

// ConsumeHandler reads records from the selected topic partition.
type ConsumeHandler struct {
	Broker             *broker.Broker
	GetCommittedOffset committedOffsetLookup
}

// ServeHTTP reads records from an explicit offset or the consumer group's committed position.
func (handler ConsumeHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodGet {
		response.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "method not allowed"})
		return
	}

	query := request.URL.Query()
	topic := query.Get("topic")
	group := query.Get("group")
	if topic == "" || group == "" {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "topic and group are required"})
		return
	}

	partition := 0
	if rawPartition := query.Get("partition"); rawPartition != "" {
		parsedPartition, err := strconv.Atoi(rawPartition)
		if err != nil || parsedPartition < 0 {
			response.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: "partition must be a non-negative integer"})
			return
		}
		partition = parsedPartition
	}

	max := 50
	if rawMax := query.Get("max"); rawMax != "" {
		parsedMax, err := strconv.Atoi(rawMax)
		if err != nil || parsedMax < 0 {
			response.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: "max must be a non-negative integer"})
			return
		}
		max = parsedMax
	}

	fromOffset := int64(0)
	if query.Has("offset") {
		parsedOffset, err := strconv.ParseInt(query.Get("offset"), 10, 64)
		if err != nil || parsedOffset < 0 {
			response.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: "offset must be a non-negative integer"})
			return
		}
		fromOffset = parsedOffset
	} else {
		if handler.GetCommittedOffset == nil {
			response.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: "committed offset lookup is not configured"})
			return
		}
		committedOffset, err := handler.GetCommittedOffset(request.Context(), group, topic, partition)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
			return
		}
		fromOffset = committedOffset
	}

	messages, err := handler.Broker.Consume(topic, partition, fromOffset, max)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(response).Encode(consumeResponse{
		Topic: topic, Partition: partition, FromOffset: fromOffset, Messages: messages,
	})
}
