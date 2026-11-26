package kafka

import "fmt"

type KafkaBatchAppender func(topic string, partition int, batch []byte) (int64, error)

// HandleProduceRequestV3 validates, appends, and answers one non-transactional Produce v3 request.
func HandleProduceRequestV3(requestFrame []byte, maxFrameBytes int32, appendBatch KafkaBatchAppender) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle Produce v3 request header: %w", err)
	}
	if header.APIKey != 0 {
		return nil, fmt.Errorf("handle Produce v3 request: API key %d, want 0", header.APIKey)
	}
	if header.APIVersion != 3 {
		return nil, fmt.Errorf("handle Produce v3 request: API version %d, want 3", header.APIVersion)
	}
	if appendBatch == nil {
		return nil, fmt.Errorf("handle Produce v3 request: nil batch appender")
	}
	request, err := decodeProduceRequestV3(body)
	if err != nil {
		return nil, fmt.Errorf("handle Produce v3 request body: %w", err)
	}
	if request.TransactionalID != nil {
		return nil, fmt.Errorf("handle Produce v3 request: transactions are unsupported")
	}
	response := produceResponseV3{Topics: make([]produceTopicResponseV3, 0, len(request.Topics))}
	for _, topic := range request.Topics {
		topicResponse := produceTopicResponseV3{Name: topic.Name, Partitions: make([]producePartitionResponseV3, 0, len(topic.Partitions))}
		for _, partition := range topic.Partitions {
			partitionResponse := producePartitionResponseV3{Index: partition.Index, BaseOffset: -1, LogAppendTimeMs: -1}
			batches, err := decodeRecordBatchesV2(partition.Records)
			if err != nil {
				partitionResponse.ErrorCode = 87
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}
			valid := true
			for _, batch := range batches {
				if _, err := decodeUncompressedRecordBatchV2(batch); err != nil {
					valid = false
					break
				}
			}
			if !valid {
				partitionResponse.ErrorCode = 87
				topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
				continue
			}
			batchOffset := 0
			for index, batch := range batches {
				batchSize := int(batch.BatchLength) + 12
				encodedBatch := partition.Records[batchOffset : batchOffset+batchSize]
				baseOffset, err := appendBatch(topic.Name, int(partition.Index), encodedBatch)
				if err != nil {
					partitionResponse.ErrorCode = -1
					break
				}
				if index == 0 {
					partitionResponse.BaseOffset = baseOffset
				}
				batchOffset += batchSize
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}
	if request.RequiredAcks == 0 {
		return nil, nil
	}
	frame, err := encodeProduceResponseFrameV3(header.CorrelationID, response, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle Produce v3 response frame: %w", err)
	}
	return frame, nil
}
