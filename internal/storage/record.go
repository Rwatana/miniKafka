package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
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
