package kafka

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeProduceRequestV3(t *testing.T) {
	transactionalID := "tx"
	body := produceRequestV3BodyForTest(&transactionalID, -1, 150, []produceTopicData{
		{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: []byte{0xaa, 0xbb}}, {Index: 1, Records: nil}}},
		{Name: "events", Partitions: []producePartitionData{}},
	})
	got, err := decodeProduceRequestV3(body)
	if err != nil {
		t.Fatalf("decodeProduceRequestV3() error = %v", err)
	}
	want := produceRequestV3{
		TransactionalID: &transactionalID,
		RequiredAcks:    -1,
		TimeoutMs:       150,
		Topics: []produceTopicData{
			{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: []byte{0xaa, 0xbb}}, {Index: 1, Records: nil}}},
			{Name: "events", Partitions: []producePartitionData{}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decodeProduceRequestV3() = %+v, want %+v", got, want)
	}

	empty, err := decodeProduceRequestV3(produceRequestV3BodyForTest(nil, 1, 0, nil))
	if err != nil {
		t.Fatalf("decodeProduceRequestV3(empty) error = %v", err)
	}
	if empty.TransactionalID != nil || empty.RequiredAcks != 1 || empty.TimeoutMs != 0 || !reflect.DeepEqual(empty.Topics, []produceTopicData{}) {
		t.Errorf("empty request = %+v, want null transactional ID, acks 1, timeout 0, and no topics", empty)
	}
}

func TestDecodeProduceRequestV3RejectsMalformedBody(t *testing.T) {
	valid := produceRequestV3BodyForTest(nil, 1, 10, nil)
	withTopic := produceRequestV3BodyForTest(nil, 1, 10, []produceTopicData{{Name: "x", Partitions: []producePartitionData{}}})
	withPartition := produceRequestV3BodyForTest(nil, 1, 10, []produceTopicData{{Name: "x", Partitions: []producePartitionData{{Index: 0, Records: []byte{1}}}}})
	invalidAcks := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(invalidAcks[2:4], 2)
	negativeTopicCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(negativeTopicCount[8:12], ^uint32(0))
	tooManyTopics := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(tooManyTopics[8:12], 1)
	negativeTopicNameLength := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint16(negativeTopicNameLength[12:14], ^uint16(0))
	longTopicName := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint16(longTopicName[12:14], 6)
	invalidTopicUTF8 := produceRequestV3BodyForTest(nil, 1, 10, []produceTopicData{{Name: "\xff", Partitions: []producePartitionData{}}})
	missingPartitionCount := append(append([]byte(nil), valid[:12]...), 0, 2, 'x', 'y', 0, 0)
	binary.BigEndian.PutUint32(missingPartitionCount[8:12], 1)
	negativePartitionCount := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint32(negativePartitionCount[15:19], ^uint32(0))
	tooManyPartitions := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint32(tooManyPartitions[15:19], 1)
	invalidRecordsLength := append([]byte(nil), withPartition...)
	binary.BigEndian.PutUint32(invalidRecordsLength[23:27], ^uint32(1))
	tooLongRecords := append([]byte(nil), withPartition...)
	binary.BigEndian.PutUint32(tooLongRecords[23:27], 2)

	tests := []struct {
		name string
		body []byte
	}{
		{name: "truncated transactional ID length", body: []byte{0}},
		{name: "transactional ID length below null sentinel", body: []byte{0xff, 0xfe}},
		{name: "truncated transactional ID", body: []byte{0, 2, 'x'}},
		{name: "invalid transactional ID UTF-8", body: []byte{0, 1, 0xff}},
		{name: "missing acknowledgments and timeout", body: []byte{0xff, 0xff}},
		{name: "invalid acknowledgments", body: invalidAcks},
		{name: "missing topic count", body: valid[:8]},
		{name: "negative topic count", body: negativeTopicCount},
		{name: "topic count exceeds available fields", body: tooManyTopics},
		{name: "negative topic name length", body: negativeTopicNameLength},
		{name: "topic name length exceeds available bytes", body: longTopicName},
		{name: "invalid topic name UTF-8", body: invalidTopicUTF8},
		{name: "missing partition count", body: missingPartitionCount},
		{name: "negative partition count", body: negativePartitionCount},
		{name: "partition count exceeds available fields", body: tooManyPartitions},
		{name: "record length below null sentinel", body: invalidRecordsLength},
		{name: "record length exceeds available bytes", body: tooLongRecords},
		{name: "trailing bytes", body: append(append([]byte(nil), valid...), 0xff)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeProduceRequestV3(test.body); err == nil {
				t.Error("decodeProduceRequestV3() error = nil, want malformed-body error")
			}
		})
	}
}

func produceRequestV3BodyForTest(transactionalID *string, acks int16, timeout int32, topics []produceTopicData) []byte {
	body := make([]byte, 0)
	if transactionalID == nil {
		body = append(body, 0xff, 0xff)
	} else {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(*transactionalID)))
		body = append(body, length[:]...)
		body = append(body, *transactionalID...)
	}
	var field [4]byte
	var short [2]byte
	binary.BigEndian.PutUint16(short[:], uint16(acks))
	body = append(body, short[:]...)
	binary.BigEndian.PutUint32(field[:], uint32(timeout))
	body = append(body, field[:]...)
	binary.BigEndian.PutUint32(field[:], uint32(len(topics)))
	body = append(body, field[:]...)
	for _, topic := range topics {
		binary.BigEndian.PutUint16(short[:], uint16(len(topic.Name)))
		body = append(body, short[:]...)
		body = append(body, topic.Name...)
		binary.BigEndian.PutUint32(field[:], uint32(len(topic.Partitions)))
		body = append(body, field[:]...)
		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint32(field[:], uint32(partition.Index))
			body = append(body, field[:]...)
			if partition.Records == nil {
				body = append(body, 0xff, 0xff, 0xff, 0xff)
			} else {
				binary.BigEndian.PutUint32(field[:], uint32(len(partition.Records)))
				body = append(body, field[:]...)
				body = append(body, partition.Records...)
			}
		}
	}
	return bytes.Clone(body)
}
