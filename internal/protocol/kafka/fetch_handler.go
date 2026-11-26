package kafka

import "fmt"

var encodeFetchHandlerResponseBody = encodeFetchResponseV4

type KafkaBatchReader func(topic string, partition int, fetchOffset int64, maxBytes int32) ([]byte, int64, error)

// HandleFetchRequestV4 reads requested partition batches and frames a Kafka Fetch v4 response.
func HandleFetchRequestV4(requestFrame []byte, maxFrameBytes int32, readBatches KafkaBatchReader) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v4 request header: %w", err)
	}
	if header.APIKey != 1 {
		return nil, fmt.Errorf("handle Fetch v4 request: API key %d, want 1", header.APIKey)
	}
	if header.APIVersion != 4 {
		return nil, fmt.Errorf("handle Fetch v4 request: API version %d, want 4", header.APIVersion)
	}
	if readBatches == nil {
		return nil, fmt.Errorf("handle Fetch v4 request: nil Kafka batch reader")
	}
	request, err := decodeFetchRequestV4(body)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v4 request body: %w", err)
	}

	response := fetchResponseV4{Topics: make([]fetchTopicResponseV4, 0, len(request.Topics))}
	for _, topic := range request.Topics {
		topicResponse := fetchTopicResponseV4{Name: topic.Name, Partitions: make([]fetchPartitionResponseV4, 0, len(topic.Partitions))}
		for _, partition := range topic.Partitions {
			partitionResponse := fetchPartitionResponseV4{Index: partition.Index}
			records, highWatermark, err := readBatches(topic.Name, int(partition.Index), partition.FetchOffset, partition.PartitionMaxBytes)
			if err != nil {
				partitionResponse.ErrorCode = -1
				partitionResponse.HighWatermark = -1
				partitionResponse.LastStableOffset = -1
			} else {
				partitionResponse.HighWatermark = highWatermark
				partitionResponse.LastStableOffset = highWatermark
				partitionResponse.Records = records
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}

	responseBody, err := encodeFetchHandlerResponseBody(response)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v4 response body: %w", err)
	}
	frameBody := append(encodeResponseHeaderV0(header.CorrelationID), responseBody...)
	frame, err := encodeResponseFrame(frameBody, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle Fetch v4 response frame: %w", err)
	}
	return frame, nil
}
