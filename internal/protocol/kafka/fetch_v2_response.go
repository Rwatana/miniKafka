package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

type fetchResponseV2 struct {
	ThrottleTime int32
	Topics       []fetchTopicResponseV2
}

type fetchTopicResponseV2 struct {
	Name       string
	Partitions []fetchPartitionResponseV2
}

type fetchPartitionResponseV2 struct {
	Index         int32
	ErrorCode     int16
	HighWatermark int64
	Records       []byte
}

// encodeFetchResponseV2 encodes the non-flexible Apache Kafka Fetch v2 response body.
func encodeFetchResponseV2(response fetchResponseV2) ([]byte, error) {
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
			return fmt.Errorf("encode Fetch v2 response: topic name is not valid UTF-8")
		}
		if len(value) > math.MaxInt16 {
			return fmt.Errorf("encode Fetch v2 response: topic name length %d exceeds INT16 maximum", len(value))
		}
		appendInt16(int16(len(value)))
		encoded = append(encoded, value...)
		return nil
	}
	appendInt32(response.ThrottleTime)
	appendInt32(int32(len(response.Topics)))
	for _, topic := range response.Topics {
		if err := appendString(topic.Name); err != nil {
			return nil, err
		}
		appendInt32(int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			appendInt32(partition.Index)
			appendInt16(partition.ErrorCode)
			appendInt64(partition.HighWatermark)
			if partition.Records == nil {
				appendInt32(-1)
			} else {
				appendInt32(int32(len(partition.Records)))
				encoded = append(encoded, partition.Records...)
			}
		}
	}
	return encoded, nil
}
