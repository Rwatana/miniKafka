package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

type listOffsetsTopicResponseV1 struct {
	Name       string
	Partitions []listOffsetsPartitionResponseV1
}

type listOffsetsPartitionResponseV1 struct {
	Index     int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// encodeListOffsetsResponseV1 encodes the legacy ListOffsets v1 topic and partition arrays.
func encodeListOffsetsResponseV1(topics []listOffsetsTopicResponseV1) ([]byte, error) {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, uint32(len(topics)))
	appendInt16 := func(value int16) {
		var field [2]byte
		binary.BigEndian.PutUint16(field[:], uint16(value))
		encoded = append(encoded, field[:]...)
	}
	appendInt32 := func(value int32) {
		var field [4]byte
		binary.BigEndian.PutUint32(field[:], uint32(value))
		encoded = append(encoded, field[:]...)
	}
	appendInt64 := func(value int64) {
		var field [8]byte
		binary.BigEndian.PutUint64(field[:], uint64(value))
		encoded = append(encoded, field[:]...)
	}
	for _, topic := range topics {
		if !utf8.ValidString(topic.Name) {
			return nil, fmt.Errorf("encode ListOffsets v1 response: topic name is not valid UTF-8")
		}
		if len(topic.Name) > math.MaxInt16 {
			return nil, fmt.Errorf("encode ListOffsets v1 response: topic name length %d exceeds INT16 maximum", len(topic.Name))
		}
		appendInt16(int16(len(topic.Name)))
		encoded = append(encoded, topic.Name...)
		appendInt32(int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			appendInt32(partition.Index)
			appendInt16(partition.ErrorCode)
			appendInt64(partition.Timestamp)
			appendInt64(partition.Offset)
		}
	}
	return encoded, nil
}
