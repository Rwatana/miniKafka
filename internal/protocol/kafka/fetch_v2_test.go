package kafka

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeFetchRequestV2(t *testing.T) {
	body := fetchRequestV2BodyForTest(fetchRequestV2{
		ReplicaID: -1, MaxWaitMs: 250, MinBytes: 1,
		Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 1, FetchOffset: 9, PartitionMaxBytes: 512}}}},
	})
	got, err := decodeFetchRequestV2(body)
	if err != nil {
		t.Fatalf("decodeFetchRequestV2() error = %v", err)
	}
	want := fetchRequestV2{
		ReplicaID: -1, MaxWaitMs: 250, MinBytes: 1,
		Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 1, FetchOffset: 9, PartitionMaxBytes: 512}}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decodeFetchRequestV2() = %+v, want %+v", got, want)
	}
	empty, err := decodeFetchRequestV2(fetchRequestV2BodyForTest(fetchRequestV2{}))
	if err != nil {
		t.Fatalf("decodeFetchRequestV2(empty) error = %v", err)
	}
	if !reflect.DeepEqual(empty.Topics, []fetchTopicRequestV4{}) {
		t.Errorf("empty topic list = %+v, want non-nil empty slice", empty.Topics)
	}
}

func TestDecodeFetchRequestV2RejectsMalformedBodies(t *testing.T) {
	empty := fetchRequestV2BodyForTest(fetchRequestV2{})
	withTopic := fetchRequestV2BodyForTest(fetchRequestV2{Topics: []fetchTopicRequestV4{{Name: "x"}}})
	withPartition := fetchRequestV2BodyForTest(fetchRequestV2{Topics: []fetchTopicRequestV4{{Name: "x", Partitions: []fetchPartitionRequestV4{{Index: 0, FetchOffset: 0, PartitionMaxBytes: 1}}}}})
	negativeWait := append([]byte(nil), empty...)
	binary.BigEndian.PutUint32(negativeWait[4:8], ^uint32(0))
	negativeMinBytes := append([]byte(nil), empty...)
	binary.BigEndian.PutUint32(negativeMinBytes[8:12], ^uint32(0))
	negativeTopicCount := append([]byte(nil), empty...)
	binary.BigEndian.PutUint32(negativeTopicCount[12:16], ^uint32(0))
	tooManyTopics := append([]byte(nil), empty...)
	binary.BigEndian.PutUint32(tooManyTopics[12:16], 1)
	negativeNameLength := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint16(negativeNameLength[16:18], ^uint16(0))
	tooLongName := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint16(tooLongName[16:18], 6)
	invalidUTF8 := fetchRequestV2BodyForTest(fetchRequestV2{Topics: []fetchTopicRequestV4{{Name: "\xff"}}})
	missingPartitionCount := withTopic[:len(withTopic)-1]
	negativePartitionCount := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint32(negativePartitionCount[len(negativePartitionCount)-4:], ^uint32(0))
	tooManyPartitions := append([]byte(nil), withTopic...)
	binary.BigEndian.PutUint32(tooManyPartitions[len(tooManyPartitions)-4:], 1)
	negativeIndex := append([]byte(nil), withPartition...)
	binary.BigEndian.PutUint32(negativeIndex[23:27], ^uint32(0))
	negativeOffset := append([]byte(nil), withPartition...)
	binary.BigEndian.PutUint64(negativeOffset[27:35], ^uint64(0))
	negativePartitionBytes := append([]byte(nil), withPartition...)
	binary.BigEndian.PutUint32(negativePartitionBytes[35:39], ^uint32(0))
	tests := []struct {
		name string
		body []byte
	}{
		{name: "short fixed fields", body: empty[:15]},
		{name: "negative max wait", body: negativeWait},
		{name: "negative minimum bytes", body: negativeMinBytes},
		{name: "negative topic count", body: negativeTopicCount},
		{name: "too many topics", body: tooManyTopics},
		{name: "negative topic name length", body: negativeNameLength},
		{name: "topic name length exceeds bytes", body: tooLongName},
		{name: "invalid topic UTF-8", body: invalidUTF8},
		{name: "missing partition count", body: missingPartitionCount},
		{name: "negative partition count", body: negativePartitionCount},
		{name: "too many partitions", body: tooManyPartitions},
		{name: "negative partition index", body: negativeIndex},
		{name: "negative fetch offset", body: negativeOffset},
		{name: "negative partition bytes", body: negativePartitionBytes},
		{name: "trailing bytes", body: append(append([]byte(nil), empty...), 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeFetchRequestV2(test.body); err == nil {
				t.Error("decodeFetchRequestV2() error = nil, want malformed-body error")
			}
		})
	}
}

func fetchRequestV2BodyForTest(request fetchRequestV2) []byte {
	body := make([]byte, 16)
	binary.BigEndian.PutUint32(body[0:4], uint32(request.ReplicaID))
	binary.BigEndian.PutUint32(body[4:8], uint32(request.MaxWaitMs))
	binary.BigEndian.PutUint32(body[8:12], uint32(request.MinBytes))
	binary.BigEndian.PutUint32(body[12:16], uint32(len(request.Topics)))
	for _, topic := range request.Topics {
		var short [2]byte
		var field [8]byte
		binary.BigEndian.PutUint16(short[:], uint16(len(topic.Name)))
		body = append(body, short[:]...)
		body = append(body, topic.Name...)
		binary.BigEndian.PutUint32(field[:4], uint32(len(topic.Partitions)))
		body = append(body, field[:4]...)
		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint32(field[:4], uint32(partition.Index))
			body = append(body, field[:4]...)
			binary.BigEndian.PutUint64(field[:], uint64(partition.FetchOffset))
			body = append(body, field[:]...)
			binary.BigEndian.PutUint32(field[:4], uint32(partition.PartitionMaxBytes))
			body = append(body, field[:4]...)
		}
	}
	return body
}
