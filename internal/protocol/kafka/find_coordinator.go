package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

// HandleFindCoordinatorRequestV1 returns the advertised broker for a group coordinator query.
func HandleFindCoordinatorRequestV1(requestFrame []byte, maxFrameBytes int32, coordinator MetadataBroker) ([]byte, error) {
	header, body, err := DecodeRequestHeaderV1(requestFrame)
	if err != nil {
		return nil, fmt.Errorf("handle FindCoordinator v1 request header: %w", err)
	}
	if header.APIKey != 10 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: API key %d, want 10", header.APIKey)
	}
	if header.APIVersion != 1 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: API version %d, want 1", header.APIVersion)
	}
	if len(body) < 3 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: body size %d is smaller than key and type", len(body))
	}
	keyLength := int16(binary.BigEndian.Uint16(body[:2]))
	if keyLength < 0 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: negative key length %d", keyLength)
	}
	if int(keyLength) > len(body)-3 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: key length %d exceeds remaining bytes", keyLength)
	}
	key := body[2 : 2+int(keyLength)]
	if !utf8.Valid(key) {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: key is not valid UTF-8")
	}
	keyType := int8(body[2+int(keyLength)])
	if keyType != 0 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: unsupported coordinator key type %d", keyType)
	}
	if 3+int(keyLength) != len(body) {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: %d trailing bytes", len(body)-(3+int(keyLength)))
	}
	if coordinator.NodeID < 0 || coordinator.Host == "" || !utf8.ValidString(coordinator.Host) || len(coordinator.Host) > math.MaxInt16 || coordinator.Port < 1 || coordinator.Port > math.MaxUint16 {
		return nil, fmt.Errorf("handle FindCoordinator v1 request: invalid coordinator endpoint")
	}
	responseBody := make([]byte, 0, 20+len(coordinator.Host))
	var int32Field [4]byte
	var int16Field [2]byte
	binary.BigEndian.PutUint32(int32Field[:], 0)
	responseBody = append(responseBody, int32Field[:]...)
	binary.BigEndian.PutUint16(int16Field[:], 0)
	responseBody = append(responseBody, int16Field[:]...)
	binary.BigEndian.PutUint16(int16Field[:], math.MaxUint16)
	responseBody = append(responseBody, int16Field[:]...)
	binary.BigEndian.PutUint32(int32Field[:], uint32(coordinator.NodeID))
	responseBody = append(responseBody, int32Field[:]...)
	binary.BigEndian.PutUint16(int16Field[:], uint16(len(coordinator.Host)))
	responseBody = append(responseBody, int16Field[:]...)
	responseBody = append(responseBody, coordinator.Host...)
	binary.BigEndian.PutUint32(int32Field[:], uint32(coordinator.Port))
	responseBody = append(responseBody, int32Field[:]...)
	frameBody := append(encodeResponseHeaderV0(header.CorrelationID), responseBody...)
	frame, err := encodeResponseFrame(frameBody, maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("handle FindCoordinator v1 response frame: %w", err)
	}
	return frame, nil
}
