package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
)

var encodeFetchV2HandlerResponseBody = encodeFetchResponseV2

// HandleFetchRequestV2 reads requested batches and frames a Kafka Fetch v2 response.
func HandleFetchRequestV2(requestFrame []byte, maxFrameBytes int32, readBatches KafkaBatchReader) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v2 request header: %w", err)
	}
	if header.APIKey != 1 {
		return nil, fmt.Errorf("handle Fetch v2 request: API key %d, want 1", header.APIKey)
	}
	if header.APIVersion != 2 {
		return nil, fmt.Errorf("handle Fetch v2 request: API version %d, want 2", header.APIVersion)
	}
	if readBatches == nil {
		return nil, fmt.Errorf("handle Fetch v2 request: nil Kafka batch reader")
	}
	request, err := decodeFetchRequestV2(body)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v2 request body: %w", err)
	}
	deadline := time.Now().Add(time.Duration(request.MaxWaitMs) * time.Millisecond)
	var response fetchResponseV2
	for {
		response = fetchResponseV2{Topics: make([]fetchTopicResponseV2, 0, len(request.Topics))}
		var returnedRecordBytes int64
		for _, topic := range request.Topics {
			topicResponse := fetchTopicResponseV2{Name: topic.Name, Partitions: make([]fetchPartitionResponseV2, 0, len(topic.Partitions))}
			for _, partition := range topic.Partitions {
				partitionResponse := fetchPartitionResponseV2{Index: partition.Index}
				records, highWatermark, readErr := readBatches(topic.Name, int(partition.Index), partition.FetchOffset, partition.PartitionMaxBytes)
				if readErr != nil {
					partitionResponse.ErrorCode = -1
					if errors.Is(readErr, kafkaerr.ErrUnknownTopicOrPartition) {
						partitionResponse.ErrorCode = 3
					}
					partitionResponse.HighWatermark = -1
				} else if partition.FetchOffset > highWatermark {
					partitionResponse.ErrorCode = 1
					partitionResponse.HighWatermark = highWatermark
				} else {
					partitionResponse.HighWatermark = highWatermark
					partitionResponse.Records = records
					returnedRecordBytes += int64(len(records))
				}
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
			}
			response.Topics = append(response.Topics, topicResponse)
		}
		if returnedRecordBytes >= int64(request.MinBytes) || request.MaxWaitMs == 0 || !time.Now().Before(deadline) {
			break
		}
		wait := 10 * time.Millisecond
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait > 0 {
			time.Sleep(wait)
		}
	}
	responseBody, err := encodeFetchV2HandlerResponseBody(response)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v2 response body: %w", err)
	}
	frameBody := append(encodeResponseHeaderV0(header.CorrelationID), responseBody...)
	frame, err := encodeResponseFrame(frameBody, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v2 response frame: %w", err)
	}
	return frame, nil
}
