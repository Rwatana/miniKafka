package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Record is the value persisted for one topic-partition log entry.
type Record struct {
	Offset    int64   `json:"offset"`
	Key       *string `json:"key"`
	Value     string  `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// encodeRecord serializes one record as newline-delimited JSON.
func encodeRecord(record Record) ([]byte, error) {
	encoded, err := json.Marshal(record)
	return append(encoded, '\n'), err
}

// decodeRecord parses one newline-delimited JSON record.
func decodeRecord(encoded []byte) (Record, error) {
	var record Record
	if err := json.Unmarshal(bytes.TrimSuffix(encoded, []byte("\n")), &record); err != nil {
		return Record{}, fmt.Errorf("decode MiniKafka record: %w", err)
	}
	return record, nil
}

// rebuildIndex returns the byte position of every record line in a partition log.
func rebuildIndex(log io.Reader) ([]int64, error) {
	reader := bufio.NewReader(log)
	var positions []int64
	var bytePosition int64
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			positions = append(positions, bytePosition)
			bytePosition += int64(len(line))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return positions, nil
			}
			return nil, fmt.Errorf("rebuild partition log index: %w", err)
		}
	}
}
