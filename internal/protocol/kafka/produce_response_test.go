package kafka

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestEncodeProduceResponseV3(t *testing.T) {
	response := produceResponseV3{
		Topics: []produceTopicResponseV3{
			{Name: "orders", Partitions: []producePartitionResponseV3{
				{Index: 0, ErrorCode: 0, BaseOffset: 12, LogAppendTimeMs: -1},
				{Index: 2, ErrorCode: 6, BaseOffset: -1, LogAppendTimeMs: -1},
			}},
			{Name: "events", Partitions: []producePartitionResponseV3{}},
		},
		ThrottleTime: 7,
	}
	got, err := encodeProduceResponseV3(response)
	if err != nil {
		t.Fatalf("encodeProduceResponseV3() error = %v", err)
	}
	want := new(bytes.Buffer)
	writeInt32 := func(value int32) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt16 := func(value int16) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt64 := func(value int64) { _ = binary.Write(want, binary.BigEndian, value) }
	writeInt32(2)
	writeInt16(6)
	want.WriteString("orders")
	writeInt32(2)
	writeInt32(0)
	writeInt16(0)
	writeInt64(12)
	writeInt64(-1)
	writeInt32(2)
	writeInt16(6)
	writeInt64(-1)
	writeInt64(-1)
	writeInt16(6)
	want.WriteString("events")
	writeInt32(0)
	writeInt32(7)
	if !reflect.DeepEqual(got, want.Bytes()) {
		t.Errorf("encoded body = %v, want %v", got, want.Bytes())
	}

	empty, err := encodeProduceResponseV3(produceResponseV3{})
	if err != nil {
		t.Fatalf("encodeProduceResponseV3(empty) error = %v", err)
	}
	if !reflect.DeepEqual(empty, []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("empty response body = %v, want two zero INT32 fields", empty)
	}
}

func TestEncodeProduceResponseV3RejectsInvalidTopicName(t *testing.T) {
	for _, test := range []struct {
		name  string
		topic string
	}{
		{name: "invalid UTF-8", topic: string([]byte{0xff})},
		{name: "too long", topic: strings.Repeat("x", 1<<15)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeProduceResponseV3(produceResponseV3{Topics: []produceTopicResponseV3{{Name: test.topic}}}); err == nil {
				t.Error("encodeProduceResponseV3() error = nil, want invalid-topic-name error")
			}
		})
	}
}

func TestEncodeProduceResponseFrameV3(t *testing.T) {
	response := produceResponseV3{Topics: []produceTopicResponseV3{{Name: "x"}}}
	got, err := encodeProduceResponseFrameV3(0x01020304, response, 19)
	if err != nil {
		t.Fatalf("encodeProduceResponseFrameV3() error = %v", err)
	}
	want := []byte{0, 0, 0, 19, 1, 2, 3, 4, 0, 0, 0, 1, 0, 1, 'x', 0, 0, 0, 0, 0, 0, 0, 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("response frame = %v, want %v", got, want)
	}
	if _, err := encodeProduceResponseFrameV3(0, response, 18); err == nil {
		t.Error("encodeProduceResponseFrameV3(oversized) error = nil, want frame-size error")
	}
	if _, err := encodeProduceResponseFrameV3(0, produceResponseV3{Topics: []produceTopicResponseV3{{Name: string([]byte{0xff})}}}, 100); err == nil {
		t.Error("encodeProduceResponseFrameV3(invalid body) error = nil, want body-encoding error")
	}
}
