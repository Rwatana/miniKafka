package kafka

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
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

func TestHandleListOffsetsRequestV1(t *testing.T) {
	body := []byte{
		0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1,
		0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 3,
		0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
		0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
	}
	request := listOffsetsRequestFrameForTest(2, 1, 42, body)
	var lookups []listOffsetsPartitionV1
	response, err := HandleListOffsetsRequestV1(request, 1024, func(topic string, partition int, timestamp int64) (int64, int64, error) {
		if topic != "orders" {
			t.Errorf("lookup topic = %q, want orders", topic)
		}
		lookups = append(lookups, listOffsetsPartitionV1{Index: int32(partition), Timestamp: timestamp})
		switch partition {
		case 0:
			return 0, -1, nil
		case 1:
			return 0, 0, kafkaerr.ErrUnknownTopicOrPartition
		default:
			return 0, 0, errors.New("storage failure")
		}
	})
	if err != nil {
		t.Fatalf("HandleListOffsetsRequestV1() error = %v", err)
	}
	wantLookups := []listOffsetsPartitionV1{{Index: 0, Timestamp: -2}, {Index: 1, Timestamp: -1}, {Index: 2, Timestamp: 0}}
	if !reflect.DeepEqual(lookups, wantLookups) {
		t.Errorf("lookup calls = %+v, want %+v", lookups, wantLookups)
	}
	wantBody, err := encodeListOffsetsResponseV1([]listOffsetsTopicResponseV1{{Name: "orders", Partitions: []listOffsetsPartitionResponseV1{
		{Index: 0, Timestamp: -1, Offset: 0},
		{Index: 1, ErrorCode: 3, Timestamp: -1, Offset: -1},
		{Index: 2, ErrorCode: -1, Timestamp: -1, Offset: -1},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := metadataExpectedResponseFrame(42, wantBody); !reflect.DeepEqual(response, want) {
		t.Errorf("response = %x, want %x", response, want)
	}
}

func TestHandleListOffsetsRequestV1RejectsInvalidRequests(t *testing.T) {
	valid := listOffsetsRequestFrameForTest(2, 1, 7, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	for _, test := range []struct {
		name   string
		frame  []byte
		max    int32
		lookup KafkaOffsetLookup
	}{
		{name: "truncated header", frame: []byte{0}, max: 1024, lookup: func(string, int, int64) (int64, int64, error) { return 0, 0, nil }},
		{name: "wrong API key", frame: listOffsetsRequestFrameForTest(1, 1, 7, nil), max: 1024, lookup: func(string, int, int64) (int64, int64, error) { return 0, 0, nil }},
		{name: "wrong API version", frame: listOffsetsRequestFrameForTest(2, 0, 7, nil), max: 1024, lookup: func(string, int, int64) (int64, int64, error) { return 0, 0, nil }},
		{name: "nil lookup", frame: valid, max: 1024},
		{name: "malformed body", frame: listOffsetsRequestFrameForTest(2, 1, 7, []byte{0}), max: 1024, lookup: func(string, int, int64) (int64, int64, error) { return 0, 0, nil }},
		{name: "response frame limit", frame: valid, max: 1, lookup: func(string, int, int64) (int64, int64, error) { return 0, 0, nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleListOffsetsRequestV1(test.frame, test.max, test.lookup); err == nil {
				t.Error("HandleListOffsetsRequestV1() error = nil, want error")
			}
		})
	}
	t.Run("response encoding failure", func(t *testing.T) {
		original := encodeListOffsetsHandlerResponseBody
		encodeListOffsetsHandlerResponseBody = func([]listOffsetsTopicResponseV1) ([]byte, error) { return nil, errors.New("encode failure") }
		defer func() { encodeListOffsetsHandlerResponseBody = original }()
		lookup := func(string, int, int64) (int64, int64, error) { return 0, 0, nil }
		if _, err := HandleListOffsetsRequestV1(valid, 1024, lookup); err == nil {
			t.Error("HandleListOffsetsRequestV1() error = nil, want encoding error")
		}
	})
}

func listOffsetsRequestFrameForTest(apiKey, apiVersion int16, correlationID int32, body []byte) []byte {
	frame := make([]byte, 10, 10+len(body))
	binary.BigEndian.PutUint16(frame[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(frame[2:4], uint16(apiVersion))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	binary.BigEndian.PutUint16(frame[8:10], 0xffff)
	return append(frame, body...)
}
