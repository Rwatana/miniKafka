package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type fetchRequestV2 struct {
	ReplicaID int32
	MaxWaitMs int32
	MinBytes  int32
	Topics    []fetchTopicRequestV4
}

// decodeFetchRequestV2 decodes Kafka's non-flexible Fetch v2 request body.
func decodeFetchRequestV2(body []byte) (fetchRequestV2, error) {
	if len(body) < 16 {
		return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 request: body size %d is smaller than fixed header and topic count", len(body))
	}
	request := fetchRequestV2{
		ReplicaID: int32(binary.BigEndian.Uint32(body[0:4])),
		MaxWaitMs: int32(binary.BigEndian.Uint32(body[4:8])),
		MinBytes:  int32(binary.BigEndian.Uint32(body[8:12])),
	}
	if request.MaxWaitMs < 0 || request.MinBytes < 0 {
		return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 request: wait and minimum byte limits must be non-negative")
	}
	topicCount := int32(binary.BigEndian.Uint32(body[12:16]))
	if topicCount < 0 {
		return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 request: negative topic count %d", topicCount)
	}
	if topicCount > int32((len(body)-16)/6) {
		return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 request: topic count %d exceeds remaining fields", topicCount)
	}
	request.Topics = make([]fetchTopicRequestV4, 0, topicCount)
	cursor := 16
	for range int(topicCount) {
		nameLength := int16(binary.BigEndian.Uint16(body[cursor : cursor+2]))
		cursor += 2
		if nameLength < 0 {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic: negative name length %d", nameLength)
		}
		if int(nameLength) > len(body)-cursor {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic: name length %d exceeds remaining bytes", nameLength)
		}
		nameBytes := body[cursor : cursor+int(nameLength)]
		if !utf8.Valid(nameBytes) {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic: name is not valid UTF-8")
		}
		topic := fetchTopicRequestV4{Name: string(nameBytes)}
		cursor += int(nameLength)
		if len(body)-cursor < 4 {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic %q: missing partition count", topic.Name)
		}
		partitionCount := int32(binary.BigEndian.Uint32(body[cursor : cursor+4]))
		cursor += 4
		if partitionCount < 0 {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic %q: negative partition count %d", topic.Name, partitionCount)
		}
		if partitionCount > int32((len(body)-cursor)/16) {
			return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic %q: partition count %d exceeds remaining fields", topic.Name, partitionCount)
		}
		topic.Partitions = make([]fetchPartitionRequestV4, 0, partitionCount)
		for range int(partitionCount) {
			partition := fetchPartitionRequestV4{
				Index:             int32(binary.BigEndian.Uint32(body[cursor : cursor+4])),
				FetchOffset:       int64(binary.BigEndian.Uint64(body[cursor+4 : cursor+12])),
				PartitionMaxBytes: int32(binary.BigEndian.Uint32(body[cursor+12 : cursor+16])),
			}
			cursor += 16
			if partition.Index < 0 || partition.FetchOffset < 0 || partition.PartitionMaxBytes < 0 {
				return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 topic %q: partition fields must be non-negative", topic.Name)
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		request.Topics = append(request.Topics, topic)
	}
	if cursor != len(body) {
		return fetchRequestV2{}, fmt.Errorf("decode Fetch v2 request: %d trailing bytes", len(body)-cursor)
	}
	return request, nil
}
