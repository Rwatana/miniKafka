package kafka

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestEncodeFetchResponseV4(t *testing.T) {
	response := fetchResponseV4{
		ThrottleTime: 5,
		Topics: []fetchTopicResponseV4{
			{Name: "orders", Partitions: []fetchPartitionResponseV4{
				{Index: 0, HighWatermark: 17, LastStableOffset: 15, AbortedTransactions: nil, Records: []byte{0xaa, 0xbb}},
				{Index: 1, ErrorCode: 6, HighWatermark: 22, LastStableOffset: 22, AbortedTransactions: []fetchAbortedTransactionV4{}, Records: nil},
				{Index: 2, HighWatermark: 31, LastStableOffset: 29, AbortedTransactions: []fetchAbortedTransactionV4{{ProducerID: 9, FirstOffset: 28}}, Records: []byte{}},
			}},
			{Name: "events", Partitions: []fetchPartitionResponseV4{}},
		},
	}
	got, err := encodeFetchResponseV4(response)
	if err != nil {
		t.Fatalf("encodeFetchResponseV4() error = %v", err)
	}
	want := new(bytes.Buffer)
	writeInt16 := func(value int16) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt32 := func(value int32) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt64 := func(value int64) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt32(5)
	writeInt32(2)
	writeInt16(6)
	want.WriteString("orders")
	writeInt32(3)
	writeInt32(0)
	writeInt16(0)
	writeInt64(17)
	writeInt64(15)
	writeInt32(-1)
	writeInt32(2)
	want.Write([]byte{0xaa, 0xbb})
	writeInt32(1)
	writeInt16(6)
	writeInt64(22)
	writeInt64(22)
	writeInt32(0)
	writeInt32(-1)
	writeInt32(2)
	writeInt16(0)
	writeInt64(31)
	writeInt64(29)
	writeInt32(1)
	writeInt64(9)
	writeInt64(28)
	writeInt32(0)
	writeInt16(6)
	want.WriteString("events")
	writeInt32(0)
	if !reflect.DeepEqual(got, want.Bytes()) {
		t.Errorf("encoded response = %x, want %x", got, want.Bytes())
	}
	empty, err := encodeFetchResponseV4(fetchResponseV4{})
	if err != nil {
		t.Fatalf("encodeFetchResponseV4(empty) error = %v", err)
	}
	if !reflect.DeepEqual(empty, []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("empty response = %x, want two empty INT32 fields", empty)
	}
}

func TestEncodeFetchResponseV4RejectsInvalidTopicName(t *testing.T) {
	for _, test := range []struct {
		name  string
		topic string
	}{
		{name: "invalid UTF-8", topic: string([]byte{0xff})},
		{name: "too long", topic: strings.Repeat("x", 1<<15)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeFetchResponseV4(fetchResponseV4{Topics: []fetchTopicResponseV4{{Name: test.topic}}}); err == nil {
				t.Error("encodeFetchResponseV4() error = nil, want invalid-topic error")
			}
		})
	}
}
