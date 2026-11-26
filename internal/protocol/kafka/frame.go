// Package kafka implements the Apache Kafka wire protocol.
package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// RequestHeader contains the fields shared by Kafka requests before the API body.
type RequestHeader struct {
	// APIKey identifies the Kafka API requested by the client.
	APIKey int16
	// APIVersion selects the schema for the requested API.
	APIVersion int16
	// CorrelationID lets the client match a response to this request.
	CorrelationID int32
	// ClientID is the optional client identifier sent in the request header.
	ClientID *string
}

// ReadRequestFrame reads one size-delimited Kafka request body from reader.
func ReadRequestFrame(reader io.Reader, maxFrameBytes int32) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("read Kafka request frame: nil reader")
	}
	if maxFrameBytes <= 0 {
		return nil, fmt.Errorf("read Kafka request frame: invalid maximum size %d", maxFrameBytes)
	}

	var sizePrefix [4]byte
	read, err := io.ReadFull(reader, sizePrefix[:])
	if err != nil {
		if read == 0 && errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read Kafka request size: %w", err)
	}

	frameSize := int32(binary.BigEndian.Uint32(sizePrefix[:]))
	if frameSize <= 0 {
		return nil, fmt.Errorf("read Kafka request frame: invalid size %d", frameSize)
	}
	if frameSize > maxFrameBytes {
		return nil, fmt.Errorf("read Kafka request frame: size %d exceeds maximum %d", frameSize, maxFrameBytes)
	}

	frame := make([]byte, frameSize)
	if _, err := io.ReadFull(reader, frame); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("read Kafka request body: %w", err)
	}
	return frame, nil
}

// DecodeRequestHeaderV1 decodes a Kafka request header v1 and returns its body.
func DecodeRequestHeaderV1(frame []byte) (RequestHeader, []byte, error) {
	if len(frame) < 8 {
		return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v1: fixed header is %d bytes, want at least 8", len(frame))
	}
	header := RequestHeader{
		APIKey:        int16(binary.BigEndian.Uint16(frame[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(frame[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(frame[4:8])),
	}
	if len(frame) < 10 {
		return RequestHeader{}, nil, errors.New("decode Kafka request header v1: missing client ID length")
	}

	clientIDLength := int16(binary.BigEndian.Uint16(frame[8:10]))
	bodyStart := 10
	if clientIDLength < -1 {
		return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v1: invalid client ID length %d", clientIDLength)
	}
	if clientIDLength >= 0 {
		clientEnd := bodyStart + int(clientIDLength)
		if clientEnd > len(frame) {
			return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v1: client ID length %d exceeds remaining bytes %d", clientIDLength, len(frame)-bodyStart)
		}
		clientIDBytes := frame[bodyStart:clientEnd]
		if !utf8.Valid(clientIDBytes) {
			return RequestHeader{}, nil, errors.New("decode Kafka request header v1: client ID is not valid UTF-8")
		}
		clientID := string(clientIDBytes)
		header.ClientID = &clientID
		bodyStart = clientEnd
	}
	return header, frame[bodyStart:], nil
}

// DecodeRequestHeaderV2 decodes a flexible Kafka request header and returns its body.
func DecodeRequestHeaderV2(frame []byte) (RequestHeader, []byte, error) {
	if len(frame) < 8 {
		return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v2: fixed header is %d bytes, want at least 8", len(frame))
	}
	header := RequestHeader{
		APIKey:        int16(binary.BigEndian.Uint16(frame[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(frame[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(frame[4:8])),
	}
	clientID, clientIDBytes, err := decodeCompactNullableString(frame[8:])
	if err != nil {
		return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v2 client ID: %w", err)
	}
	header.ClientID = clientID
	tagBytes, err := skipTaggedFields(frame[8+clientIDBytes:])
	if err != nil {
		return RequestHeader{}, nil, fmt.Errorf("decode Kafka request header v2 tags: %w", err)
	}
	bodyStart := 8 + clientIDBytes + tagBytes
	return header, frame[bodyStart:], nil
}

// encodeResponseHeaderV0 encodes one Kafka response correlation ID.
func encodeResponseHeaderV0(correlationID int32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(correlationID))
	return encoded[:]
}

// encodeResponseFrame prefixes a Kafka response body with its bounded INT32 size.
func encodeResponseFrame(body []byte, maxFrameBytes int32) ([]byte, error) {
	if maxFrameBytes <= 0 {
		return nil, fmt.Errorf("encode Kafka response frame: invalid maximum size %d", maxFrameBytes)
	}
	if len(body) > int(maxFrameBytes) {
		return nil, fmt.Errorf("encode Kafka response frame: size %d exceeds maximum %d", len(body), maxFrameBytes)
	}

	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(int32(len(body))))
	copy(frame[4:], body)
	return frame, nil
}
