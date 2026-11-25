package storage

import (
	"bufio"
	"bytes"
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

// PartitionLog is an opened partition's log and recovered offset index.
type PartitionLog = partitionLog

// OpenPartitionLog opens or creates a log and recovers its byte index.
func OpenPartitionLog(path string) (*PartitionLog, error) {
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

// ClosePartitionLog releases the file owned by a partition log.
func ClosePartitionLog(log *PartitionLog) error {
	if log == nil || log.file == nil {
		return nil
	}
	if err := log.file.Close(); err != nil {
		return fmt.Errorf("close partition log: %w", err)
	}
	return nil
}

// AppendRecord writes one record and advances the partition's offset index.
func AppendRecord(log *PartitionLog, key *string, value string) (int64, error) {
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

// readFromOffset reads at most max log lines starting at an inclusive offset.
func readFromOffset(log *partitionLog, fromOffset int64, max int) ([]Record, error) {
	if log == nil || log.file == nil {
		return nil, errors.New("read partition log: nil log or file")
	}
	if fromOffset < 0 {
		return nil, fmt.Errorf("read partition log: negative offset %d", fromOffset)
	}
	if max < 0 {
		return nil, fmt.Errorf("read partition log: negative maximum %d", max)
	}
	if max == 0 || fromOffset >= int64(len(log.positions)) {
		return []Record{}, nil
	}

	remaining := int64(len(log.positions)) - fromOffset
	count := int64(max)
	if count > remaining {
		count = remaining
	}
	if _, err := log.file.Seek(log.positions[fromOffset], io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek partition log offset %d: %w", fromOffset, err)
	}

	reader := bufio.NewReader(log.file)
	records := make([]Record, 0, count)
	for lineNumber := int64(0); lineNumber < count; lineNumber++ {
		line, err := reader.ReadBytes('\n')
		if len(line) == 0 && errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read partition log offset %d: %w", fromOffset+lineNumber, err)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		record, err := decodeRecord(line)
		if err != nil {
			return nil, fmt.Errorf("decode partition log offset %d: %w", fromOffset+lineNumber, err)
		}
		records = append(records, record)
	}
	return records, nil
}

// latestOffset returns the next offset that would be assigned to a record.
func latestOffset(log *partitionLog) (int64, error) {
	if log == nil || log.file == nil {
		return 0, errors.New("get latest partition offset: nil log or file")
	}
	return int64(len(log.positions)), nil
}
