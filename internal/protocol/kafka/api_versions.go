package kafka

import (
	"encoding/binary"
	"fmt"
)

// HandleAPIVersionsRequestV0 validates and answers one Kafka ApiVersions v0 request frame.
func HandleAPIVersionsRequestV0(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle ApiVersions v0 request header: %w", err)
	}
	if header.APIKey != 18 {
		return nil, fmt.Errorf("handle ApiVersions v0 request: API key %d, want 18", header.APIKey)
	}
	if header.APIVersion != 0 {
		return nil, fmt.Errorf("handle ApiVersions v0 request: API version %d, want 0", header.APIVersion)
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("handle ApiVersions v0 request body: got %d bytes, want none", len(body))
	}

	response := encodeResponseHeaderV0(header.CorrelationID)
	var fields [6]byte
	var length [4]byte
	response = append(response, 0, 0)
	binary.BigEndian.PutUint32(length[:], uint32(len(supportedAPIs)))
	response = append(response, length[:]...)
	for _, apiVersion := range supportedAPIs {
		binary.BigEndian.PutUint16(fields[0:2], uint16(apiVersion.APIKey))
		binary.BigEndian.PutUint16(fields[2:4], uint16(apiVersion.MinVersion))
		binary.BigEndian.PutUint16(fields[4:6], uint16(apiVersion.MaxVersion))
		response = append(response, fields[:]...)
	}
	return encodeResponseFrame(response, maxFrameBytes)
}

type apiVersionsRequest struct {
	clientSoftwareName    string
	clientSoftwareVersion string
}

// decodeAPIVersionsRequestV3 decodes the flexible ApiVersions v3 request body.
func decodeAPIVersionsRequestV3(body []byte) (apiVersionsRequest, error) {
	name, nameBytes, err := decodeCompactString(body)
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 client software name: %w", err)
	}
	version, versionBytes, err := decodeCompactString(body[nameBytes:])
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 client software version: %w", err)
	}
	tagsStart := nameBytes + versionBytes
	tagsBytes, err := skipTaggedFields(body[tagsStart:])
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 tagged fields: %w", err)
	}
	if tagsStart+tagsBytes != len(body) {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 request: %d trailing bytes", len(body)-tagsStart-tagsBytes)
	}
	return apiVersionsRequest{clientSoftwareName: name, clientSoftwareVersion: version}, nil
}

// HandleAPIVersionsRequestV3 validates and answers one Kafka ApiVersions v3 request frame.
func HandleAPIVersionsRequestV3(requestFrame []byte, maxFrameBytes int32, supportedAPIs []APIVersionRange) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV2(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle ApiVersions v3 request header: %w", err)
	}
	if header.APIKey != 18 {
		return nil, fmt.Errorf("handle ApiVersions v3 request: API key %d, want 18", header.APIKey)
	}
	if header.APIVersion != 3 {
		return nil, fmt.Errorf("handle ApiVersions v3 request: API version %d, want 3", header.APIVersion)
	}
	if _, err := decodeAPIVersionsRequestV3(body); err != nil {
		return nil, fmt.Errorf("handle ApiVersions v3 request body: %w", err)
	}
	return encodeAPIVersionsResponseFrameV3(header.CorrelationID, 0, supportedAPIs, maxFrameBytes)
}
