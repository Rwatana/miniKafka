package storage

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestEncodeRecord(t *testing.T) {
	const timestamp int64 = 1795664096123
	key := "customer-42"
	tests := []struct {
		name   string
		record Record
	}{
		{
			name: "keyed record",
			record: Record{
				Offset:    7,
				Key:       &key,
				Value:     "order-created",
				Timestamp: timestamp,
			},
		},
		{
			name: "null key",
			record: Record{
				Offset:    8,
				Value:     "anonymous-event",
				Timestamp: timestamp,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := encodeRecord(test.record)
			if err != nil {
				t.Fatalf("encodeRecord() error = %v", err)
			}
			if !bytes.HasSuffix(encoded, []byte("\n")) {
				t.Fatalf("encoded record does not end with a newline: %q", encoded)
			}
			if got := bytes.Count(encoded, []byte("\n")); got != 1 {
				t.Errorf("newline count = %d, want 1", got)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(bytes.TrimSuffix(encoded, []byte("\n")), &fields); err != nil {
				t.Fatalf("read encoded field names: %v", err)
			}
			for _, field := range []string{"offset", "key", "value", "timestamp"} {
				if _, ok := fields[field]; !ok {
					t.Errorf("encoded record is missing field %q", field)
				}
			}
			var gotTimestamp int64
			if err := json.Unmarshal(fields["timestamp"], &gotTimestamp); err != nil {
				t.Fatalf("timestamp is not a JSON integer: %v", err)
			}
			if gotTimestamp != timestamp {
				t.Errorf("timestamp = %d, want %d", gotTimestamp, timestamp)
			}

			var decoded Record
			if err := json.Unmarshal(bytes.TrimSuffix(encoded, []byte("\n")), &decoded); err != nil {
				t.Fatalf("decode encoded record: %v", err)
			}
			if decoded.Offset != test.record.Offset {
				t.Errorf("offset = %d, want %d", decoded.Offset, test.record.Offset)
			}
			if decoded.Key == nil != (test.record.Key == nil) {
				t.Fatalf("key nil = %t, want %t", decoded.Key == nil, test.record.Key == nil)
			}
			if decoded.Key != nil && *decoded.Key != *test.record.Key {
				t.Errorf("key = %q, want %q", *decoded.Key, *test.record.Key)
			}
			if decoded.Value != test.record.Value {
				t.Errorf("value = %q, want %q", decoded.Value, test.record.Value)
			}
			if decoded.Timestamp != timestamp {
				t.Errorf("timestamp = %d, want %d", decoded.Timestamp, timestamp)
			}
		})
	}
}
