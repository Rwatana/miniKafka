package storage

import (
	"encoding/json"
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
