package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type produceRequestV3 struct {
	TransactionalID *string
	RequiredAcks    int16
	TimeoutMs       int32
	Topics          []produceTopicData
}

type produceTopicData struct {
	Name       string
	Partitions []producePartitionData
}

type producePartitionData struct {
	Index   int32
	Records []byte
}

// decodeProduceRequestV3 decodes Kafka Produce v3 fields while preserving record batches as bytes.
func decodeProduceRequestV3(body []byte) (produceRequestV3, error) {
	request := produceRequestV3{}
	offset := 0
	if len(body)-offset < 2 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: missing transactional ID length")
	}
	transactionalIDLength := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if transactionalIDLength < -1 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: invalid transactional ID length %d", transactionalIDLength)
	}
	if transactionalIDLength >= 0 {
		length := int(transactionalIDLength)
		if length > len(body)-offset {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: transactional ID length %d exceeds remaining bytes %d", length, len(body)-offset)
		}
		value := body[offset : offset+length]
		if !utf8.Valid(value) {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: transactional ID is not valid UTF-8")
		}
		transactionalID := string(value)
		request.TransactionalID = &transactionalID
		offset += length
	}
	if len(body)-offset < 6 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: missing acknowledgments or timeout")
	}
	request.RequiredAcks = int16(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if request.RequiredAcks != -1 && request.RequiredAcks != 0 && request.RequiredAcks != 1 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: invalid required acknowledgments %d", request.RequiredAcks)
	}
	request.TimeoutMs = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if len(body)-offset < 4 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: missing topic count")
	}
	topicCount := int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if topicCount < 0 {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: negative topic count %d", topicCount)
	}
	if topicCount > int32((len(body)-offset)/6) {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: topic count %d exceeds remaining fields", topicCount)
	}
	request.Topics = make([]produceTopicData, 0, topicCount)
	for range int(topicCount) {
		nameLength := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if nameLength < 0 {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic: invalid name length %d", nameLength)
		}
		if int(nameLength) > len(body)-offset {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic: name length %d exceeds remaining bytes %d", nameLength, len(body)-offset)
		}
		nameBytes := body[offset : offset+int(nameLength)]
		if !utf8.Valid(nameBytes) {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic: name is not valid UTF-8")
		}
		topic := produceTopicData{Name: string(nameBytes)}
		offset += int(nameLength)
		if len(body)-offset < 4 {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic %q: missing partition count", topic.Name)
		}
		partitionCount := int32(binary.BigEndian.Uint32(body[offset : offset+4]))
		offset += 4
		if partitionCount < 0 {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic %q: negative partition count %d", topic.Name, partitionCount)
		}
		if partitionCount > int32((len(body)-offset)/8) {
			return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic %q: partition count %d exceeds remaining fields", topic.Name, partitionCount)
		}
		topic.Partitions = make([]producePartitionData, 0, partitionCount)
		for range int(partitionCount) {
			partition := producePartitionData{Index: int32(binary.BigEndian.Uint32(body[offset : offset+4]))}
			offset += 4
			recordsLength := int32(binary.BigEndian.Uint32(body[offset : offset+4]))
			offset += 4
			if recordsLength < -1 {
				return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic %q partition %d: invalid record length %d", topic.Name, partition.Index, recordsLength)
			}
			if recordsLength >= 0 {
				length := int(recordsLength)
				if length > len(body)-offset {
					return produceRequestV3{}, fmt.Errorf("decode Produce v3 topic %q partition %d: record length %d exceeds remaining bytes %d", topic.Name, partition.Index, length, len(body)-offset)
				}
				partition.Records = body[offset : offset+length]
				offset += length
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		request.Topics = append(request.Topics, topic)
	}
	if offset != len(body) {
		return produceRequestV3{}, fmt.Errorf("decode Produce v3 request: %d trailing bytes", len(body)-offset)
	}
	return request, nil
}
