package kafka

import (
	"errors"
	"fmt"
	"net"
)

// ServeAPIVersionsV3 accepts TCP connections and serves ApiVersions v3 on each.
func ServeAPIVersionsV3(listener net.Listener, maxFrameBytes int32, supportedAPIs []APIVersionRange) error {
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("accept Kafka TCP connection: %w", err)
		}
		connection := conn
		go func() {
			_ = ServeAPIVersionsV3Connection(connection, maxFrameBytes, supportedAPIs)
			_ = connection.Close()
		}()
	}
}

// ServeKafka accepts clients and serves supported Kafka APIs on each connection.
func ServeKafka(listener net.Listener, maxFrameBytes int32, supportedAPIs []APIVersionRange, metadataProvider MetadataProvider, produceAppenders ...KafkaBatchAppender) error {
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("accept Kafka TCP connection: %w", err)
		}
		connection := conn
		go func() {
			_ = ServeKafkaConnection(connection, maxFrameBytes, supportedAPIs, metadataProvider, produceAppenders...)
			_ = connection.Close()
		}()
	}
}

// ServeKafkaWithFetchReader accepts clients and serves connections with a broker-backed Fetch reader.
func ServeKafkaWithFetchReader(listener net.Listener, maxFrameBytes int32, supportedAPIs []APIVersionRange, metadataProvider MetadataProvider, appendBatch KafkaBatchAppender, readBatches KafkaBatchReader) error {
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("accept Kafka TCP connection: %w", err)
		}
		connection := conn
		go func() {
			_ = ServeKafkaConnectionWithFetchReader(connection, maxFrameBytes, supportedAPIs, metadataProvider, appendBatch, readBatches)
			_ = connection.Close()
		}()
	}
}
