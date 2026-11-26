package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

type produceResponseV3 struct {
	Topics       []produceTopicResponseV3
	ThrottleTime int32
}

type produceTopicResponseV3 struct {
	Name       string
	Partitions []producePartitionResponseV3
}

type producePartitionResponseV3 struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
}

// encodeProduceResponseV3 encodes a non-flexible Kafka Produce v3 response body.
func encodeProduceResponseV3(response produceResponseV3) ([]byte, error) {
	encoded := make([]byte, 0)
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
	appendString := func(value string) error {
		if !utf8.ValidString(value) {
			return fmt.Errorf("encode Produce v3 response: topic name is not valid UTF-8")
		}
		if len(value) > math.MaxInt16 {
			return fmt.Errorf("encode Produce v3 response: topic name length %d exceeds INT16 maximum", len(value))
		}
		appendInt16(int16(len(value)))
		encoded = append(encoded, value...)
		return nil
	}
	appendInt32(int32(len(response.Topics)))
	for _, topic := range response.Topics {
		if err := appendString(topic.Name); err != nil {
			return nil, err
		}
		appendInt32(int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			appendInt32(partition.Index)
			appendInt16(partition.ErrorCode)
			appendInt64(partition.BaseOffset)
			appendInt64(partition.LogAppendTimeMs)
		}
	}
	appendInt32(response.ThrottleTime)
	return encoded, nil
}

// encodeProduceResponseFrameV3 assembles a bounded Produce v3 response frame.
func encodeProduceResponseFrameV3(correlationID int32, response produceResponseV3, maxFrameBytes int32) ([]byte, error) {
	body, err := encodeProduceResponseV3(response)
	if err != nil {
		return nil, err
	}
	frameBody := append(encodeResponseHeaderV0(correlationID), body...)
	return encodeResponseFrame(frameBody, maxFrameBytes)
}
