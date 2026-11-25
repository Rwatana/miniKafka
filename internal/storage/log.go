package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

var partitionLogOpenFile = os.OpenFile

type partitionLog struct {
	file      *os.File
	positions []int64
}

// openPartitionLog opens or creates a log and recovers its byte index.
func openPartitionLog(path string) (*partitionLog, error) {
	file, err := partitionLogOpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open partition log: %w", err)
	}

	positions, err := rebuildIndex(file)
	if err != nil {
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("recover partition log index: %w", err), closeErr)
	}
	return &partitionLog{file: file, positions: positions}, nil
}

// closePartitionLog releases the file owned by a partition log.
func closePartitionLog(log *partitionLog) error {
	if log == nil || log.file == nil {
		return nil
	}
	if err := log.file.Close(); err != nil {
		return fmt.Errorf("close partition log: %w", err)
	}
	return nil
}

// appendRecord writes one record and advances the partition's offset index.
func appendRecord(log *partitionLog, key *string, value string) (int64, error) {
	if log == nil || log.file == nil {
		return 0, errors.New("append partition record: nil log or file")
	}

	position, err := log.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek partition log end: %w", err)
	}

	offset := int64(len(log.positions))
	encoded, err := partitionRecordEncoder(Record{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		return 0, fmt.Errorf("encode partition record: %w", err)
	}
	if _, err := log.file.Write(encoded); err != nil {
		return 0, fmt.Errorf("append partition record: %w", err)
	}

	log.positions = append(log.positions, position)
	return offset, nil
}
