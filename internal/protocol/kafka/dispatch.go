package kafka

import (
	"encoding/binary"
	"fmt"
)

// HandleKafkaRequest dispatches one Kafka request frame to its API implementation.
func HandleKafkaRequest(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange, brokers []MetadataBroker, topics []MetadataTopic, produceAppenders ...KafkaBatchAppender) ([]byte, error) {
	if len(requestFrame) < 2 {
		return nil, fmt.Errorf("dispatch Kafka request: frame is %d bytes, want API key", len(requestFrame))
	}
	switch apiKey := int16(binary.BigEndian.Uint16(requestFrame[:2])); apiKey {
	case 3:
		return HandleMetadataRequestV0(requestFrame, maxFrameBytes, brokers, topics)
	case 0:
		var appendBatch KafkaBatchAppender
		if len(produceAppenders) > 0 {
			appendBatch = produceAppenders[0]
		}
		return HandleProduceRequestV3(requestFrame, maxFrameBytes, appendBatch)
	case 18:
		return HandleAPIVersionsRequestV3(requestFrame, maxFrameBytes, supportedAPIs)
	default:
		return nil, fmt.Errorf("dispatch Kafka request: unsupported API key %d", apiKey)
	}
}

// HandleKafkaRequestWithFetchReader routes Fetch to its reader-aware handler and preserves existing API dispatch.
func HandleKafkaRequestWithFetchReader(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange, brokers []MetadataBroker, topics []MetadataTopic, appendBatch KafkaBatchAppender, readBatches KafkaBatchReader) ([]byte, error) {
	if len(requestFrame) >= 2 && int16(binary.BigEndian.Uint16(requestFrame[:2])) == 1 {
		return HandleFetchRequestV4(requestFrame, maxFrameBytes, readBatches)
	}
	if appendBatch == nil {
		return HandleKafkaRequest(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics)
	}
	return HandleKafkaRequest(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics, appendBatch)
}
