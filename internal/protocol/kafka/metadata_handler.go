package kafka

import "fmt"

// HandleMetadataRequestV0 answers one Kafka Metadata v0 request with the supplied broker state.
func HandleMetadataRequestV0(requestFrame []byte, maxFrameBytes int32, brokers []MetadataBroker, availableTopics []MetadataTopic) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v0 request header: %w", err)
	}
	if header.APIKey != 3 {
		return nil, fmt.Errorf("handle Metadata v0 request: API key %d, want 3", header.APIKey)
	}
	if header.APIVersion != 0 {
		return nil, fmt.Errorf("handle Metadata v0 request: API version %d, want 0", header.APIVersion)
	}

	requestedTopics, err := decodeMetadataRequestV0(body)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v0 request body: %w", err)
	}
	responseTopics := make([]MetadataTopic, 0, len(requestedTopics))
	if len(requestedTopics) == 0 {
		responseTopics = append(responseTopics, availableTopics...)
	} else {
		knownTopics := make(map[string]MetadataTopic, len(availableTopics))
		for _, topic := range availableTopics {
			knownTopics[topic.Name] = topic
		}
		for _, name := range requestedTopics {
			if topic, exists := knownTopics[name]; exists {
				responseTopics = append(responseTopics, topic)
			} else {
				responseTopics = append(responseTopics, MetadataTopic{ErrorCode: 3, Name: name})
			}
		}
	}

	responseBody, err := encodeMetadataResponseV0(brokers, responseTopics)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v0 response body: %w", err)
	}
	response := append(encodeResponseHeaderV0(header.CorrelationID), responseBody...)
	responseFrame, err := encodeResponseFrame(response, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v0 response frame: %w", err)
	}
	return responseFrame, nil
}

// HandleMetadataRequestV1 answers one Kafka Metadata v1 request with the supplied broker state.
func HandleMetadataRequestV1(requestFrame []byte, maxFrameBytes int32, brokers []MetadataBroker, availableTopics []MetadataTopic) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v1 request header: %w", err)
	}
	if header.APIKey != 3 {
		return nil, fmt.Errorf("handle Metadata v1 request: API key %d, want 3", header.APIKey)
	}
	if header.APIVersion != 1 {
		return nil, fmt.Errorf("handle Metadata v1 request: API version %d, want 1", header.APIVersion)
	}
	requestedTopics, err := decodeMetadataRequestV0(body)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v1 request body: %w", err)
	}
	responseTopics := make([]MetadataTopic, 0, len(requestedTopics))
	if len(requestedTopics) == 0 {
		responseTopics = append(responseTopics, availableTopics...)
	} else {
		knownTopics := make(map[string]MetadataTopic, len(availableTopics))
		for _, topic := range availableTopics {
			knownTopics[topic.Name] = topic
		}
		for _, name := range requestedTopics {
			if topic, exists := knownTopics[name]; exists {
				responseTopics = append(responseTopics, topic)
			} else {
				responseTopics = append(responseTopics, MetadataTopic{ErrorCode: 3, Name: name})
			}
		}
	}
	controllerID := int32(-1)
	if len(brokers) > 0 {
		controllerID = brokers[0].NodeID
	}
	responseBody, err := encodeMetadataResponseV1(brokers, controllerID, responseTopics)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v1 response body: %w", err)
	}
	response := append(encodeResponseHeaderV0(header.CorrelationID), responseBody...)
	responseFrame, err := encodeResponseFrame(response, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle Metadata v1 response frame: %w", err)
	}
	return responseFrame, nil
}
