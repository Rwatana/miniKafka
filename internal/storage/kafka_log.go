package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

var copyKafkaBatchChecksum = io.CopyN
var writeKafkaBatch = func(file *os.File, batch []byte) (int, error) { return file.Write(batch) }
var syncKafkaPartitionLog = func(file *os.File) error { return file.Sync() }

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

// RecoverKafkaPartitionLog validates stored batches and restores their next partition offset.
func RecoverKafkaPartitionLog(log *KafkaPartitionLog) error {
	if log == nil || log.file == nil {
		return fmt.Errorf("recover Kafka partition log: nil log")
	}
	info, err := log.file.Stat()
	if err != nil {
		return fmt.Errorf("stat Kafka partition log: %w", err)
	}
	fileSize := info.Size()
	var position int64
	var nextOffset int64
	for position < fileSize {
		remaining := fileSize - position
		if remaining < 61 {
			return fmt.Errorf("recover Kafka partition log: truncated batch header at byte %d", position)
		}
		var header [61]byte
		if _, err := log.file.ReadAt(header[:], position); err != nil {
			return fmt.Errorf("read Kafka batch header at byte %d: %w", position, err)
		}
		batchLength := int32(binary.BigEndian.Uint32(header[8:12]))
		if batchLength < 49 {
			return fmt.Errorf("recover Kafka partition log: invalid batch length %d at byte %d", batchLength, position)
		}
		encodedLength := int64(batchLength) + 12
		if encodedLength > remaining {
			return fmt.Errorf("recover Kafka partition log: batch at byte %d declares %d bytes, only %d remain", position, encodedLength, remaining)
		}
		if header[16] != 2 {
			return fmt.Errorf("recover Kafka partition log: unsupported magic %d at byte %d", header[16], position)
		}
		checksum := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		if _, err := copyKafkaBatchChecksum(checksum, io.NewSectionReader(log.file, position+21, encodedLength-21), encodedLength-21); err != nil {
			return fmt.Errorf("read Kafka batch checksum input at byte %d: %w", position, err)
		}
		wantChecksum := binary.BigEndian.Uint32(header[17:21])
		if checksum.Sum32() != wantChecksum {
			return fmt.Errorf("recover Kafka partition log: CRC32C mismatch at byte %d", position)
		}
		baseOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		lastOffsetDelta := int32(binary.BigEndian.Uint32(header[23:27]))
		recordsCount := int32(binary.BigEndian.Uint32(header[57:61]))
		if baseOffset < 0 || recordsCount < 0 || lastOffsetDelta < 0 {
			return fmt.Errorf("recover Kafka partition log: invalid offset or record count at byte %d", position)
		}
		if baseOffset < nextOffset {
			return fmt.Errorf("recover Kafka partition log: batch base offset %d precedes next offset %d", baseOffset, nextOffset)
		}
		if baseOffset > math.MaxInt64-int64(lastOffsetDelta)-1 {
			return fmt.Errorf("recover Kafka partition log: offset overflow at byte %d", position)
		}
		nextOffset = baseOffset + int64(lastOffsetDelta) + 1
		position += encodedLength
	}
	log.nextOffset = nextOffset
	return nil
}

// AppendKafkaRecordBatch assigns the next base offset and durably appends one validated batch.
func AppendKafkaRecordBatch(log *KafkaPartitionLog, batch []byte) (int64, error) {
	if log == nil || log.file == nil {
		return 0, fmt.Errorf("append Kafka record batch: nil log")
	}
	if len(batch) < 61 {
		return 0, fmt.Errorf("append Kafka record batch: size %d is smaller than header size 61", len(batch))
	}
	batchLength := int32(binary.BigEndian.Uint32(batch[8:12]))
	if batchLength < 49 || int64(batchLength)+12 != int64(len(batch)) {
		return 0, fmt.Errorf("append Kafka record batch: invalid batch length %d for size %d", batchLength, len(batch))
	}
	if batch[16] != 2 {
		return 0, fmt.Errorf("append Kafka record batch: unsupported magic %d", batch[16])
	}
	lastOffsetDelta := int32(binary.BigEndian.Uint32(batch[23:27]))
	if lastOffsetDelta < 0 || int32(binary.BigEndian.Uint32(batch[57:61])) < 0 {
		return 0, fmt.Errorf("append Kafka record batch: negative offset delta or record count")
	}
	if log.nextOffset > math.MaxInt64-int64(lastOffsetDelta)-1 {
		return 0, fmt.Errorf("append Kafka record batch: partition offset overflow")
	}
	info, err := log.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat Kafka partition log before append: %w", err)
	}
	baseOffset := log.nextOffset
	encoded := append([]byte(nil), batch...)
	binary.BigEndian.PutUint64(encoded[0:8], uint64(baseOffset))
	written, err := writeKafkaBatch(log.file, encoded)
	if err != nil || written != len(encoded) {
		_ = log.file.Truncate(info.Size())
		if err != nil {
			return 0, fmt.Errorf("append Kafka record batch: %w", err)
		}
		return 0, io.ErrShortWrite
	}
	if err := syncKafkaPartitionLog(log.file); err != nil {
		_ = log.file.Truncate(info.Size())
		return 0, fmt.Errorf("sync Kafka record batch: %w", err)
	}
	log.nextOffset = baseOffset + int64(lastOffsetDelta) + 1
	return baseOffset, nil
}

// CloseKafkaPartitionLog releases the file owned by a Kafka partition-log handle.
func CloseKafkaPartitionLog(log *KafkaPartitionLog) error {
	if log == nil || log.file == nil {
		return fmt.Errorf("close Kafka partition log: nil log")
	}
	if err := log.file.Close(); err != nil {
		return fmt.Errorf("close Kafka partition log: %w", err)
	}
	return nil
}
