// Package kafka implements the Apache Kafka wire protocol.
package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

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
