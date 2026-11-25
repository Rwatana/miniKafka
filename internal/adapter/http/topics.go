package httpadapter

import (
	"encoding/json"
	"net/http"

	"github.com/Rwatana/miniKafka/internal/broker"
)

type topicPartitionReader interface {
	ListPartitions() ([]broker.PartitionInfo, error)
	LatestOffset(string, int) (int64, error)
}

type topicResponse struct {
	Topic        string `json:"topic"`
	Partition    int    `json:"partition"`
	LatestOffset int64  `json:"latestOffset"`
}

// TopicsHandler lists the topic partitions and their current end offsets.
type TopicsHandler struct {
	Broker topicPartitionReader
}

// ServeHTTP returns every known partition with its next offset.
func (handler TopicsHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodGet {
		response.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: "method not allowed"})
		return
	}

	partitions, err := handler.Broker.ListPartitions()
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
		return
	}
	topics := make([]topicResponse, 0, len(partitions))
	for _, partition := range partitions {
		latestOffset, err := handler.Broker.LatestOffset(partition.Topic, partition.Partition)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(response).Encode(errorResponse{Error: err.Error()})
			return
		}
		topics = append(topics, topicResponse{
			Topic: partition.Topic, Partition: partition.Partition, LatestOffset: latestOffset,
		})
	}
	_ = json.NewEncoder(response).Encode(topics)
}
