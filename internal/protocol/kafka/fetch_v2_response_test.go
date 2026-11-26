package kafka

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestEncodeFetchResponseV2(t *testing.T) {
	response := fetchResponseV2{
		ThrottleTime: 4,
		Topics: []fetchTopicResponseV2{{Name: "orders", Partitions: []fetchPartitionResponseV2{
			{Index: 0, HighWatermark: 7, Records: []byte{0xaa, 0xbb}},
			{Index: 1, ErrorCode: 6, HighWatermark: 9, Records: nil},
			{Index: 2, HighWatermark: 10, Records: []byte{}},
		}}},
	}
	got, err := encodeFetchResponseV2(response)
	if err != nil {
		t.Fatalf("encodeFetchResponseV2() error = %v", err)
	}
	want := new(bytes.Buffer)
	writeInt16 := func(value int16) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt32 := func(value int32) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt64 := func(value int64) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt32(4)
	writeInt32(1)
	writeInt16(6)
	want.WriteString("orders")
	writeInt32(3)
	writeInt32(0)
	writeInt16(0)
	writeInt64(7)
	writeInt32(2)
	want.Write([]byte{0xaa, 0xbb})
	writeInt32(1)
	writeInt16(6)
	writeInt64(9)
	writeInt32(-1)
	writeInt32(2)
	writeInt16(0)
	writeInt64(10)
	writeInt32(0)
	if !bytes.Equal(got, want.Bytes()) {
		t.Errorf("encoded response = %x, want %x", got, want.Bytes())
	}
	empty, err := encodeFetchResponseV2(fetchResponseV2{})
	if err != nil {
		t.Fatalf("encodeFetchResponseV2(empty) error = %v", err)
	}
	if !bytes.Equal(empty, []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("empty response = %x, want throttle and empty topic count", empty)
	}
}

func TestEncodeFetchResponseV2RejectsInvalidTopicNames(t *testing.T) {
	for _, test := range []struct {
		name  string
		topic string
	}{
		{name: "invalid UTF-8", topic: string([]byte{0xff})},
		{name: "too long", topic: strings.Repeat("x", 1<<15)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeFetchResponseV2(fetchResponseV2{Topics: []fetchTopicResponseV2{{Name: test.topic}}}); err == nil {
				t.Error("encodeFetchResponseV2() error = nil, want invalid-topic error")
			}
		})
	}
}
