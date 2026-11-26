package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type fetchRequestV4 struct {
	ReplicaID      int32
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []fetchTopicRequestV4
}

type fetchTopicRequestV4 struct {
	Name       string
	Partitions []fetchPartitionRequestV4
}

type fetchPartitionRequestV4 struct {
	Index             int32
	FetchOffset       int64
	PartitionMaxBytes int32
}

// decodeFetchRequestV4 decodes Apache Kafka's non-flexible Fetch v4 request body.
func decodeFetchRequestV4(body []byte) (fetchRequestV4, error) {
	if len(body) < 21 {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: body size %d is smaller than fixed header and topic count", len(body))
	}
	request := fetchRequestV4{
		ReplicaID:      int32(binary.BigEndian.Uint32(body[0:4])),
		MaxWaitMs:      int32(binary.BigEndian.Uint32(body[4:8])),
		MinBytes:       int32(binary.BigEndian.Uint32(body[8:12])),
		MaxBytes:       int32(binary.BigEndian.Uint32(body[12:16])),
		IsolationLevel: int8(body[16]),
	}
	if request.MaxWaitMs < 0 || request.MinBytes < 0 || request.MaxBytes < 0 {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: wait and byte limits must be non-negative")
	}
	if request.IsolationLevel != 0 && request.IsolationLevel != 1 {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: invalid isolation level %d", request.IsolationLevel)
	}
	topicCount := int32(binary.BigEndian.Uint32(body[17:21]))
	if topicCount < 0 {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: negative topic count %d", topicCount)
	}
	if topicCount > int32((len(body)-21)/6) {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: topic count %d exceeds remaining fields", topicCount)
	}
	request.Topics = make([]fetchTopicRequestV4, 0, topicCount)
	offset := 21
	for range int(topicCount) {
		nameLength := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if nameLength < 0 {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic: negative name length %d", nameLength)
		}
		if int(nameLength) > len(body)-offset {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic: name length %d exceeds remaining bytes", nameLength)
		}
		nameBytes := body[offset : offset+int(nameLength)]
		if !utf8.Valid(nameBytes) {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic: name is not valid UTF-8")
		}
		topic := fetchTopicRequestV4{Name: string(nameBytes)}
		offset += int(nameLength)
		if len(body)-offset < 4 {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic %q: missing partition count", topic.Name)
		}
		partitionCount := int32(binary.BigEndian.Uint32(body[offset : offset+4]))
		offset += 4
		if partitionCount < 0 {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic %q: negative partition count %d", topic.Name, partitionCount)
		}
		if partitionCount > int32((len(body)-offset)/16) {
			return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic %q: partition count %d exceeds remaining fields", topic.Name, partitionCount)
		}
		topic.Partitions = make([]fetchPartitionRequestV4, 0, partitionCount)
		for range int(partitionCount) {
			partition := fetchPartitionRequestV4{
				Index:             int32(binary.BigEndian.Uint32(body[offset : offset+4])),
				FetchOffset:       int64(binary.BigEndian.Uint64(body[offset+4 : offset+12])),
				PartitionMaxBytes: int32(binary.BigEndian.Uint32(body[offset+12 : offset+16])),
			}
			offset += 16
			if partition.Index < 0 || partition.FetchOffset < 0 || partition.PartitionMaxBytes < 0 {
				return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 topic %q: partition fields must be non-negative", topic.Name)
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		request.Topics = append(request.Topics, topic)
	}
	if offset != len(body) {
		return fetchRequestV4{}, fmt.Errorf("decode Fetch v4 request: %d trailing bytes", len(body)-offset)
	}
	return request, nil
}
