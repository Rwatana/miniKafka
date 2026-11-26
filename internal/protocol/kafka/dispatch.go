package kafka

import (
	"encoding/binary"
	"fmt"
)

// HandleKafkaRequest dispatches one Kafka request frame to its API implementation.
func HandleKafkaRequest(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange, brokers []MetadataBroker, topics []MetadataTopic) ([]byte, error) {
	if len(requestFrame) < 2 {
		return nil, fmt.Errorf("dispatch Kafka request: frame is %d bytes, want API key", len(requestFrame))
	}
	switch apiKey := int16(binary.BigEndian.Uint16(requestFrame[:2])); apiKey {
	case 3:
		return HandleMetadataRequestV0(requestFrame, maxFrameBytes, brokers, topics)
	case 18:
		return HandleAPIVersionsRequestV3(requestFrame, maxFrameBytes, supportedAPIs)
	default:
		return nil, fmt.Errorf("dispatch Kafka request: unsupported API key %d", apiKey)
	}
}
