package kafka

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncodeListOffsetsResponseV1(t *testing.T) {
	tests := []struct {
		name   string
		topics []listOffsetsTopicResponseV1
		want   []byte
	}{
		{
			name: "one topic and partition",
			topics: []listOffsetsTopicResponseV1{{Name: "orders", Partitions: []listOffsetsPartitionResponseV1{{
				Index: 0, ErrorCode: 0, Timestamp: -1, Offset: 9,
			}}}},
			want: []byte{
				0, 0, 0, 1, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 1,
				0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0, 0, 0, 0, 0, 0, 0, 9,
			},
		},
		{name: "empty response", topics: []listOffsetsTopicResponseV1{}, want: []byte{0, 0, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := encodeListOffsetsResponseV1(test.topics)
			if err != nil {
				t.Fatalf("encodeListOffsetsResponseV1() error = %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Errorf("response = %x, want %x", got, test.want)
			}
		})
	}
	t.Run("rejects invalid UTF-8", func(t *testing.T) {
		if _, err := encodeListOffsetsResponseV1([]listOffsetsTopicResponseV1{{Name: "\xff"}}); err == nil {
			t.Error("encodeListOffsetsResponseV1() error = nil, want UTF-8 error")
		}
	})
	t.Run("rejects oversized STRING", func(t *testing.T) {
		if _, err := encodeListOffsetsResponseV1([]listOffsetsTopicResponseV1{{Name: strings.Repeat("x", 32768)}}); err == nil {
			t.Error("encodeListOffsetsResponseV1() error = nil, want oversized string error")
		}
	})
}
