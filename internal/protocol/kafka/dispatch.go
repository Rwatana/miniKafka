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
		if len(requestFrame) < 4 {
			return nil, fmt.Errorf("dispatch Kafka Metadata request: frame is %d bytes, want API version", len(requestFrame))
		}
		switch apiVersion := int16(binary.BigEndian.Uint16(requestFrame[2:4])); apiVersion {
		case 0:
			return HandleMetadataRequestV0(requestFrame, maxFrameBytes, brokers, topics)
		case 1:
			return HandleMetadataRequestV1(requestFrame, maxFrameBytes, brokers, topics)
		default:
			return nil, fmt.Errorf("dispatch Kafka Metadata request: unsupported API version %d", apiVersion)
		}
	case 0:
		var appendBatch KafkaBatchAppender
		if len(produceAppenders) > 0 {
			appendBatch = produceAppenders[0]
		}
		return HandleProduceRequestV3(requestFrame, maxFrameBytes, appendBatch)
	case 18:
		if len(requestFrame) < 4 {
			return nil, fmt.Errorf("dispatch Kafka ApiVersions request: frame is %d bytes, want API version", len(requestFrame))
		}
		switch apiVersion := int16(binary.BigEndian.Uint16(requestFrame[2:4])); apiVersion {
		case 0:
			return HandleAPIVersionsRequestV0(requestFrame, maxFrameBytes, supportedAPIs)
		case 3:
			return HandleAPIVersionsRequestV3(requestFrame, maxFrameBytes, supportedAPIs)
		default:
			return nil, fmt.Errorf("dispatch Kafka ApiVersions request: unsupported API version %d", apiVersion)
		}
	default:
		return nil, fmt.Errorf("dispatch Kafka request: unsupported API key %d", apiKey)
	}
}

// HandleKafkaRequestWithFetchReader routes Fetch versions and ListOffsets to their broker-aware handlers and preserves existing API dispatch.
func HandleKafkaRequestWithFetchReader(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange, brokers []MetadataBroker, topics []MetadataTopic, appendBatch KafkaBatchAppender, readBatches KafkaBatchReader, offsetLookups ...KafkaOffsetLookup) ([]byte, error) {
	if len(requestFrame) >= 2 && int16(binary.BigEndian.Uint16(requestFrame[:2])) == 1 {
		if len(requestFrame) >= 4 && int16(binary.BigEndian.Uint16(requestFrame[2:4])) == 2 {
			return HandleFetchRequestV2(requestFrame, maxFrameBytes, readBatches)
		}
		return HandleFetchRequestV4(requestFrame, maxFrameBytes, readBatches)
	}
	if len(requestFrame) >= 2 && int16(binary.BigEndian.Uint16(requestFrame[:2])) == 2 {
		var lookup KafkaOffsetLookup
		if len(offsetLookups) > 0 {
			lookup = offsetLookups[0]
		}
		return HandleListOffsetsRequestV1(requestFrame, maxFrameBytes, lookup)
	}
	if appendBatch == nil {
		return HandleKafkaRequest(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics)
	}
	return HandleKafkaRequest(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics, appendBatch)
}
