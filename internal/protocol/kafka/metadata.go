package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

// MetadataBroker describes one broker advertised by a Metadata response.
type MetadataBroker struct {
	// NodeID is the stable ID of the broker.
	NodeID int32
	// Host is the broker's advertised hostname.
	Host string
	// Port is the broker's advertised TCP port.
	Port int32
}

// MetadataPartition describes one topic partition and its replica placement.
type MetadataPartition struct {
	// ErrorCode is the Kafka partition error code.
	ErrorCode int16
	// PartitionIndex is the partition's index within its topic.
	PartitionIndex int32
	// LeaderID is the broker ID of the partition leader.
	LeaderID int32
	// ReplicaNodes lists all brokers hosting the partition.
	ReplicaNodes []int32
	// ISRNodes lists the in-sync replica broker IDs.
	ISRNodes []int32
}

// MetadataTopic describes one topic and its partitions in a Metadata response.
type MetadataTopic struct {
	// ErrorCode is the Kafka topic error code.
	ErrorCode int16
	// Name is the topic's UTF-8 name.
	Name string
	// Partitions lists the topic's partitions.
	Partitions []MetadataPartition
}

// decodeMetadataRequestV0 decodes the legacy topic array in a Metadata v0 request.
func decodeMetadataRequestV0(body []byte) ([]string, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("decode Metadata v0 request: missing topic count")
	}
	count := int32(binary.BigEndian.Uint32(body[:4]))
	if count == -1 {
		return []string{}, nil
	}
	if count < 0 {
		return nil, fmt.Errorf("decode Metadata v0 request: negative topic count %d", count)
	}
	if int64(count) > int64((len(body)-4)/2) {
		return nil, fmt.Errorf("decode Metadata v0 request: topic count %d exceeds remaining minimum fields", count)
	}
	topics := make([]string, 0)
	cursor := 4
	for index := int32(0); index < count; index++ {
		length := int16(binary.BigEndian.Uint16(body[cursor : cursor+2]))
		cursor += 2
		if length < 0 {
			return nil, fmt.Errorf("decode Metadata v0 request: negative topic %d length %d", index, length)
		}
		end := cursor + int(length)
		if end > len(body) {
			return nil, fmt.Errorf("decode Metadata v0 request: topic %d length %d exceeds remaining bytes %d", index, length, len(body)-cursor)
		}
		nameBytes := body[cursor:end]
		if !utf8.Valid(nameBytes) {
			return nil, fmt.Errorf("decode Metadata v0 request: topic %d name is not valid UTF-8", index)
		}
		topics = append(topics, string(nameBytes))
		cursor = end
	}
	if cursor != len(body) {
		return nil, fmt.Errorf("decode Metadata v0 request: %d trailing bytes", len(body)-cursor)
	}
	return topics, nil
}

// encodeMetadataResponseV0 encodes brokers, topics, and partitions in legacy Metadata v0 format.
func encodeMetadataResponseV0(brokers []MetadataBroker, topics []MetadataTopic) ([]byte, error) {
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
	appendString := func(value string) error {
		if !utf8.ValidString(value) {
			return fmt.Errorf("encode Metadata v0 response: string is not valid UTF-8")
		}
		if len(value) > math.MaxInt16 {
			return fmt.Errorf("encode Metadata v0 response: string length %d exceeds INT16 maximum", len(value))
		}
		appendInt16(int16(len(value)))
		encoded = append(encoded, value...)
		return nil
	}
	appendInt32(int32(len(brokers)))
	for _, broker := range brokers {
		appendInt32(broker.NodeID)
		if err := appendString(broker.Host); err != nil {
			return nil, err
		}
		appendInt32(broker.Port)
	}
	appendInt32(int32(len(topics)))
	for _, topic := range topics {
		appendInt16(topic.ErrorCode)
		if err := appendString(topic.Name); err != nil {
			return nil, err
		}
		appendInt32(int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			appendInt16(partition.ErrorCode)
			appendInt32(partition.PartitionIndex)
			appendInt32(partition.LeaderID)
			appendInt32(int32(len(partition.ReplicaNodes)))
			for _, replica := range partition.ReplicaNodes {
				appendInt32(replica)
			}
			appendInt32(int32(len(partition.ISRNodes)))
			for _, replica := range partition.ISRNodes {
				appendInt32(replica)
			}
		}
	}
	return encoded, nil
}

// encodeMetadataResponseV1 encodes brokers, controller, topics, and partitions in Metadata v1 format.
func encodeMetadataResponseV1(brokers []MetadataBroker, controllerID int32, topics []MetadataTopic) ([]byte, error) {
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
	appendString := func(value string) error {
		if !utf8.ValidString(value) {
			return fmt.Errorf("encode Metadata v1 response: string is not valid UTF-8")
		}
		if len(value) > math.MaxInt16 {
			return fmt.Errorf("encode Metadata v1 response: string length %d exceeds INT16 maximum", len(value))
		}
		appendInt16(int16(len(value)))
		encoded = append(encoded, value...)
		return nil
	}
	appendInt32(int32(len(brokers)))
	for _, broker := range brokers {
		appendInt32(broker.NodeID)
		if err := appendString(broker.Host); err != nil {
			return nil, err
		}
		appendInt32(broker.Port)
		appendInt16(-1) // Rack is nullable and not configured by MiniKafka.
	}
	appendInt32(controllerID)
	appendInt32(int32(len(topics)))
	for _, topic := range topics {
		appendInt16(topic.ErrorCode)
		if err := appendString(topic.Name); err != nil {
			return nil, err
		}
		encoded = append(encoded, 0) // is_internal is false for user topics.
		appendInt32(int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			appendInt16(partition.ErrorCode)
			appendInt32(partition.PartitionIndex)
			appendInt32(partition.LeaderID)
			appendInt32(int32(len(partition.ReplicaNodes)))
			for _, replica := range partition.ReplicaNodes {
				appendInt32(replica)
			}
			appendInt32(int32(len(partition.ISRNodes)))
			for _, replica := range partition.ISRNodes {
				appendInt32(replica)
			}
		}
	}
	return encoded, nil
}
