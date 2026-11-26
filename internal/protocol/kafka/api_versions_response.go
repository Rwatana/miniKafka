package kafka

import "encoding/binary"

type apiVersionRange struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

// encodeAPIVersionsResponseV3 encodes a flexible Kafka ApiVersions response body.
func encodeAPIVersionsResponseV3(errorCode int16, apiVersions []apiVersionRange) []byte {
	encoded := make([]byte, 2, 2+5+len(apiVersions)*6+5)
	binary.BigEndian.PutUint16(encoded, uint16(errorCode))
	var compactLength [binary.MaxVarintLen32]byte
	lengthBytes := binary.PutUvarint(compactLength[:], uint64(len(apiVersions)+1))
	encoded = append(encoded, compactLength[:lengthBytes]...)
	for _, apiVersion := range apiVersions {
		var fields [6]byte
		binary.BigEndian.PutUint16(fields[0:2], uint16(apiVersion.apiKey))
		binary.BigEndian.PutUint16(fields[2:4], uint16(apiVersion.minVersion))
		binary.BigEndian.PutUint16(fields[4:6], uint16(apiVersion.maxVersion))
		encoded = append(encoded, fields[:]...)
	}
	encoded = append(encoded, 0, 0, 0, 0, 0)
	return encoded
}

// encodeAPIVersionsResponseFrameV3 assembles a complete bounded v3 response frame.
func encodeAPIVersionsResponseFrameV3(correlationID int32, errorCode int16, apiVersions []apiVersionRange, maxFrameBytes int32) ([]byte, error) {
	response := encodeResponseHeaderV0(correlationID)
	response = append(response, encodeAPIVersionsResponseV3(errorCode, apiVersions)...)
	return encodeResponseFrame(response, maxFrameBytes)
}
