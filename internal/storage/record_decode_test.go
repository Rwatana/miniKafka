package storage

import (
	"testing"
)

func TestDecodeRecord(t *testing.T) {
	key := "customer-42"
	const timestamp int64 = 1795664096123
	tests := []struct {
		name    string
		encoded string
		want    Record
	}{
		{
			name:    "keyed record with newline",
			encoded: "{\"offset\":7,\"key\":\"customer-42\",\"value\":\"order-created\",\"timestamp\":1795664096123}\n",
			want: Record{
				Offset:    7,
				Key:       &key,
				Value:     "order-created",
				Timestamp: timestamp,
			},
		},
		{
			name:    "null key without newline",
			encoded: "{\"offset\":8,\"key\":null,\"value\":\"anonymous-event\",\"timestamp\":1795664096123}",
			want: Record{
				Offset:    8,
				Value:     "anonymous-event",
				Timestamp: timestamp,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeRecord([]byte(test.encoded))
			if err != nil {
				t.Fatalf("decodeRecord() error = %v", err)
			}
			if got.Offset != test.want.Offset {
				t.Errorf("offset = %d, want %d", got.Offset, test.want.Offset)
			}
			if got.Key == nil != (test.want.Key == nil) {
				t.Fatalf("key nil = %t, want %t", got.Key == nil, test.want.Key == nil)
			}
			if got.Key != nil && *got.Key != *test.want.Key {
				t.Errorf("key = %q, want %q", *got.Key, *test.want.Key)
			}
			if got.Value != test.want.Value {
				t.Errorf("value = %q, want %q", got.Value, test.want.Value)
			}
			if got.Timestamp != test.want.Timestamp {
				t.Errorf("timestamp = %d, want %d", got.Timestamp, test.want.Timestamp)
			}
		})
	}

	t.Run("malformed JSON", func(t *testing.T) {
		got, err := decodeRecord([]byte("{\"offset\":99,\"value\":"))
		if err == nil {
			t.Fatal("decodeRecord() error = nil, want malformed JSON error")
		}
		if got != (Record{}) {
			t.Errorf("record = %+v, want zero value after decode failure", got)
		}
	})
}
