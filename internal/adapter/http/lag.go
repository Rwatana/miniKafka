package httpadapter

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/infrastructure/postgres"
)

type groupOffsetsLookup func(context.Context, string) ([]GroupOffset, error)
type partitionLatestOffset func(string, int) (int64, error)

// GroupOffset is one committed position returned by a lag lookup.
type GroupOffset = postgres.GroupOffset

type lagResponse struct {
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	CommittedOffset int64  `json:"committedOffset"`
	LatestOffset    int64  `json:"latestOffset"`
	Lag             int64  `json:"lag"`
}

// LagHandler reports per-partition consumer lag for one group.
type LagHandler struct {
	ListOffsetsForGroup groupOffsetsLookup
	LatestOffset        partitionLatestOffset
}

// ServeHTTP returns the committed offset, log end, and lag for each partition in a group.
func (handler LagHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodGet {
		response.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "method not allowed"})
		return
	}
	group := request.URL.Query().Get("group")
	if group == "" {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "group is required"})
		return
	}
	if handler.ListOffsetsForGroup == nil || handler.LatestOffset == nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "lag dependencies are not configured"})
		return
	}

	committedOffsets, err := handler.ListOffsetsForGroup(request.Context(), group)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
		return
	}
	results := make([]lagResponse, 0, len(committedOffsets))
	for _, committed := range committedOffsets {
		latest, err := handler.LatestOffset(committed.Topic, committed.Partition)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
			return
		}
		results = append(results, lagResponse{
			Topic: committed.Topic, Partition: committed.Partition,
			CommittedOffset: committed.CommittedOffset, LatestOffset: latest,
			Lag: broker.CalculateLag(latest, committed.CommittedOffset),
		})
	}
	_ = json.NewEncoder(response).Encode(results)
}
