package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// MetadataProvider loads the current broker and topic metadata for one Kafka request.
type MetadataProvider func() ([]MetadataBroker, []MetadataTopic, error)

// ServeAPIVersionsV3Connection serves ordered ApiVersions v3 frames until the connection closes.
func ServeAPIVersionsV3Connection(conn net.Conn, maxFrameBytes int32, supportedAPIs []APIVersionRange) error {
	for {
		requestFrame, err := ReadRequestFrame(conn, maxFrameBytes)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read Kafka ApiVersions request: %w", err)
		}
		response, err := HandleAPIVersionsRequestV3(requestFrame, maxFrameBytes, supportedAPIs)
		if err != nil {
			return fmt.Errorf("handle Kafka ApiVersions request: %w", err)
		}
		for len(response) > 0 {
			written, err := conn.Write(response)
			if err != nil {
				return fmt.Errorf("write Kafka ApiVersions response: %w", err)
			}
			if written == 0 {
				return io.ErrShortWrite
			}
			response = response[written:]
		}
	}
}

// ServeKafkaConnection processes ordered Kafka API request frames until the connection closes.
func ServeKafkaConnection(conn net.Conn, maxFrameBytes int32, supportedAPIs []APIVersionRange, metadataProvider MetadataProvider, produceAppenders ...KafkaBatchAppender) error {
	for {
		requestFrame, err := ReadRequestFrame(conn, maxFrameBytes)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read Kafka request: %w", err)
		}
		var brokers []MetadataBroker
		var topics []MetadataTopic
		if len(requestFrame) >= 2 && int16(binary.BigEndian.Uint16(requestFrame[:2])) == 3 && metadataProvider != nil {
			brokers, topics, err = metadataProvider()
			if err != nil {
				return fmt.Errorf("load Kafka metadata: %w", err)
			}
		}
		response, err := HandleKafkaRequest(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics, produceAppenders...)
		if err != nil {
			return fmt.Errorf("handle Kafka request: %w", err)
		}
		for len(response) > 0 {
			written, err := conn.Write(response)
			if err != nil {
				return fmt.Errorf("write Kafka response: %w", err)
			}
			if written == 0 {
				return io.ErrShortWrite
			}
			response = response[written:]
		}
	}
}

// ServeKafkaConnectionWithFetchReader processes requests through the reader-aware Fetch dispatcher until EOF.
func ServeKafkaConnectionWithFetchReader(conn net.Conn, maxFrameBytes int32, supportedAPIs []APIVersionRange, metadataProvider MetadataProvider, appendBatch KafkaBatchAppender, readBatches KafkaBatchReader) error {
	for {
		requestFrame, err := ReadRequestFrame(conn, maxFrameBytes)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read Kafka request: %w", err)
		}
		var brokers []MetadataBroker
		var topics []MetadataTopic
		if len(requestFrame) >= 2 && int16(binary.BigEndian.Uint16(requestFrame[:2])) == 3 && metadataProvider != nil {
			brokers, topics, err = metadataProvider()
			if err != nil {
				return fmt.Errorf("load Kafka metadata: %w", err)
			}
		}
		response, err := HandleKafkaRequestWithFetchReader(requestFrame, maxFrameBytes, supportedAPIs, brokers, topics, appendBatch, readBatches)
		if err != nil {
			return fmt.Errorf("handle Kafka request: %w", err)
		}
		for len(response) > 0 {
			written, err := conn.Write(response)
			if err != nil {
				return fmt.Errorf("write Kafka response: %w", err)
			}
			if written == 0 {
				return io.ErrShortWrite
			}
			response = response[written:]
		}
	}
}
