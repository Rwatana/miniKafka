package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type listOffsetsRequestV1 struct {
	ReplicaID int32
	Topics    []listOffsetsTopicV1
}

type listOffsetsTopicV1 struct {
	Name       string
	Partitions []listOffsetsPartitionV1
}

type listOffsetsPartitionV1 struct {
	Index     int32
	Timestamp int64
}

// decodeListOffsetsRequestV1 decodes the legacy ListOffsets v1 request body.
func decodeListOffsetsRequestV1(body []byte) (listOffsetsRequestV1, error) {
	if len(body) < 8 {
		return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 request: body size %d is smaller than fixed header and topic count", len(body))
	}
	request := listOffsetsRequestV1{ReplicaID: int32(binary.BigEndian.Uint32(body[:4]))}
	topicCount := int32(binary.BigEndian.Uint32(body[4:8]))
	if topicCount < 0 {
		return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 request: negative topic count %d", topicCount)
	}
	if topicCount > int32((len(body)-8)/6) {
		return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 request: topic count %d exceeds remaining fields", topicCount)
	}
	request.Topics = make([]listOffsetsTopicV1, 0, topicCount)
	cursor := 8
	for range int(topicCount) {
		if len(body)-cursor < 2 {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic: missing name length")
		}
		nameLength := int16(binary.BigEndian.Uint16(body[cursor : cursor+2]))
		cursor += 2
		if nameLength < 0 {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic: negative name length %d", nameLength)
		}
		if int(nameLength) > len(body)-cursor {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic: name length %d exceeds remaining bytes", nameLength)
		}
		nameBytes := body[cursor : cursor+int(nameLength)]
		if !utf8.Valid(nameBytes) {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic: name is not valid UTF-8")
		}
		topic := listOffsetsTopicV1{Name: string(nameBytes)}
		cursor += int(nameLength)
		if len(body)-cursor < 4 {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic %q: missing partition count", topic.Name)
		}
		partitionCount := int32(binary.BigEndian.Uint32(body[cursor : cursor+4]))
		cursor += 4
		if partitionCount < 0 {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic %q: negative partition count %d", topic.Name, partitionCount)
		}
		if partitionCount > int32((len(body)-cursor)/12) {
			return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic %q: partition count %d exceeds remaining fields", topic.Name, partitionCount)
		}
		topic.Partitions = make([]listOffsetsPartitionV1, 0, partitionCount)
		for range int(partitionCount) {
			partition := listOffsetsPartitionV1{
				Index:     int32(binary.BigEndian.Uint32(body[cursor : cursor+4])),
				Timestamp: int64(binary.BigEndian.Uint64(body[cursor+4 : cursor+12])),
			}
			cursor += 12
			if partition.Index < 0 {
				return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 topic %q: negative partition index %d", topic.Name, partition.Index)
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		request.Topics = append(request.Topics, topic)
	}
	if cursor != len(body) {
		return listOffsetsRequestV1{}, fmt.Errorf("decode ListOffsets v1 request: %d trailing bytes", len(body)-cursor)
	}
	return request, nil
}
