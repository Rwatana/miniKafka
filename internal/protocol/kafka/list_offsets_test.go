package kafka

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeListOffsetsRequestV1(t *testing.T) {
	body := []byte{
		0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1,
		0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 2,
		0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
		0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
	want := listOffsetsRequestV1{
		ReplicaID: -1,
		Topics: []listOffsetsTopicV1{{Name: "orders", Partitions: []listOffsetsPartitionV1{
			{Index: 0, Timestamp: -2},
			{Index: 1, Timestamp: -1},
		}}},
	}
	got, err := decodeListOffsetsRequestV1(body)
	if err != nil {
		t.Fatalf("decodeListOffsetsRequestV1() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("request = %+v, want %+v", got, want)
	}

	emptyBody := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if got, err := decodeListOffsetsRequestV1(emptyBody); err != nil || !reflect.DeepEqual(got, listOffsetsRequestV1{Topics: []listOffsetsTopicV1{}}) {
		t.Errorf("decodeListOffsetsRequestV1(empty) = (%+v, %v), want empty request", got, err)
	}
}

func TestDecodeListOffsetsRequestV1RejectsMalformedBodies(t *testing.T) {
	base := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	makeTopic := func(nameLength int16, name []byte, partitionCount int32, partitions []byte) []byte {
		body := append([]byte(nil), base...)
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(nameLength))
		body = append(body, length[:]...)
		body = append(body, name...)
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], uint32(partitionCount))
		body = append(body, count[:]...)
		return append(body, partitions...)
	}
	missingNameLength := []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 0}
	missingPartitionCount := []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 'x', 0, 0, 0, 0, 0, 0, 0, 0, 0}
	negativeIndex := makeTopic(0, nil, 1, []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0})
	tests := []struct {
		name string
		body []byte
	}{
		{name: "short fixed fields", body: []byte{0, 0, 0, 0}},
		{name: "negative topic count", body: []byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}},
		{name: "topic count exceeds available fields", body: base},
		{name: "missing second topic name length", body: missingNameLength},
		{name: "negative topic name length", body: makeTopic(-1, nil, 0, nil)},
		{name: "topic name exceeds remaining bytes", body: makeTopic(5, nil, 0, nil)},
		{name: "invalid UTF-8 topic name", body: makeTopic(1, []byte{0xff}, 0, nil)},
		{name: "missing partition count", body: missingPartitionCount},
		{name: "negative partition count", body: makeTopic(0, nil, -1, nil)},
		{name: "partition count exceeds remaining fields", body: makeTopic(0, nil, 1, nil)},
		{name: "negative partition index", body: negativeIndex},
		{name: "trailing bytes", body: append(makeTopic(0, nil, 0, nil), 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeListOffsetsRequestV1(test.body); err == nil {
				t.Error("decodeListOffsetsRequestV1() error = nil, want error")
			}
		})
	}
}
