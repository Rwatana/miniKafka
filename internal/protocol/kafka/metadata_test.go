package kafka

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeMetadataRequestV0(t *testing.T) {
	tests := []struct {
		name  string
		body  []byte
		want  []string
		valid bool
	}{
		{name: "empty topics", body: []byte{0, 0, 0, 0}, want: []string{}, valid: true},
		{name: "multiple UTF-8 topics", body: []byte{0, 0, 0, 2, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 3, 0xe7, 0x8c, 0xab}, want: []string{"orders", "猫"}, valid: true},
		{name: "truncated array count", body: []byte{0, 0}, valid: false},
		{name: "negative topic count", body: []byte{0xff, 0xff, 0xff, 0xff}, valid: false},
		{name: "count exceeds available fields", body: []byte{0, 0, 0, 1}, valid: false},
		{name: "negative topic length", body: []byte{0, 0, 0, 1, 0xff, 0xff}, valid: false},
		{name: "truncated topic bytes", body: []byte{0, 0, 0, 1, 0, 3, 'x'}, valid: false},
		{name: "invalid UTF-8", body: []byte{0, 0, 0, 1, 0, 1, 0xff}, valid: false},
		{name: "trailing bytes", body: []byte{0, 0, 0, 0, 0xff}, valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeMetadataRequestV0(test.body)
			if (err == nil) != test.valid {
				t.Fatalf("decodeMetadataRequestV0() error = %v, valid = %v", err, test.valid)
			}
			if test.valid && !reflect.DeepEqual(got, test.want) {
				t.Errorf("topics = %v, want %v", got, test.want)
			}
		})
	}
}

func TestEncodeMetadataResponseV0(t *testing.T) {
	partition := MetadataPartition{ErrorCode: 0, PartitionIndex: 0, LeaderID: 7, ReplicaNodes: []int32{7}, ISRNodes: []int32{7}}
	tests := []struct {
		name    string
		brokers []MetadataBroker
		topics  []MetadataTopic
		want    []byte
	}{
		{
			name:    "one broker topic and partition",
			brokers: []MetadataBroker{{NodeID: 7, Host: "broker-1", Port: 9092}},
			topics:  []MetadataTopic{{Name: "orders", Partitions: []MetadataPartition{partition}}},
			want: []byte{
				0, 0, 0, 1, 0, 0, 0, 7, 0, 8, 'b', 'r', 'o', 'k', 'e', 'r', '-', '1', 0, 0, 0x23, 0x84,
				0, 0, 0, 1, 0, 0, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 1,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 0, 7,
			},
		},
		{name: "empty metadata", brokers: []MetadataBroker{}, topics: []MetadataTopic{}, want: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := encodeMetadataResponseV0(test.brokers, test.topics)
			if err != nil {
				t.Fatalf("encodeMetadataResponseV0() error = %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Errorf("response = %x, want %x", got, test.want)
			}
		})
	}

	t.Run("rejects invalid UTF-8", func(t *testing.T) {
		if _, err := encodeMetadataResponseV0([]MetadataBroker{{Host: "\xff"}}, nil); err == nil {
			t.Error("encodeMetadataResponseV0() error = nil, want invalid UTF-8 error")
		}
	})
	t.Run("rejects invalid topic name UTF-8", func(t *testing.T) {
		if _, err := encodeMetadataResponseV0(nil, []MetadataTopic{{Name: "\xff"}}); err == nil {
			t.Error("encodeMetadataResponseV0() error = nil, want invalid topic name UTF-8 error")
		}
	})
	t.Run("rejects oversized STRING", func(t *testing.T) {
		if _, err := encodeMetadataResponseV0([]MetadataBroker{{Host: strings.Repeat("x", 32768)}}, nil); err == nil {
			t.Error("encodeMetadataResponseV0() error = nil, want oversized STRING error")
		}
	})
}
