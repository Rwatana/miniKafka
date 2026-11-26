package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// KafkaPartitionLog is an appendable binary log dedicated to Kafka record batches.
type KafkaPartitionLog struct {
	file       *os.File
	nextOffset int64
}

// OpenKafkaPartitionLog creates or opens one appendable binary Kafka partition log.
func OpenKafkaPartitionLog(path string) (*KafkaPartitionLog, error) {
	if path == "" {
		return nil, fmt.Errorf("open Kafka partition log: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create Kafka partition log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Kafka partition log %q: %w", path, err)
	}
	return &KafkaPartitionLog{file: file}, nil
}
