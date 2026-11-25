package httpadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type offsetCommitter func(context.Context, string, string, int, int64) error

type commitRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition *int   `json:"partition"`
	Offset    *int64 `json:"offset"`
}

type commitResponse struct {
	Committed bool   `json:"committed"`
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// CommitHandler persists a consumer group's processed position.
type CommitHandler struct {
	CommitOffset offsetCommitter
}

// ServeHTTP validates and stores one consumer group partition offset.
func (handler CommitHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodPost {
		response.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "method not allowed"})
		return
	}

	var input commitRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "invalid JSON request"})
		return
	}
	if strings.TrimSpace(input.Group) == "" || strings.TrimSpace(input.Topic) == "" || input.Offset == nil {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "group, topic, and offset are required"})
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
	if *input.Offset < 0 {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "offset must be non-negative"})
		return
	}
	if handler.CommitOffset == nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "offset commit is not configured"})
		return
	}
	if err := handler.CommitOffset(request.Context(), input.Group, input.Topic, partition, *input.Offset); err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
		return
	}

	_ = json.NewEncoder(response).Encode(commitResponse{
		Committed: true, Group: input.Group, Topic: input.Topic, Partition: partition, Offset: *input.Offset,
	})
}
