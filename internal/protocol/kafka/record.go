package kafka

import (
	"fmt"
	"unicode/utf8"
)

type recordHeaderV2 struct {
	Key   string
	Value []byte
}

type kafkaRecordV2 struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []recordHeaderV2
}

// decodeRecordV2 decodes one Kafka magic-v2 record and returns its encoded size.
func decodeRecordV2(encoded []byte) (kafkaRecordV2, int, error) {
	recordLength, lengthBytes, err := decodeSignedVarint(encoded)
	if err != nil {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2 length: %w", err)
	}
	if recordLength < 0 {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: negative length %d", recordLength)
	}
	if int64(recordLength) > int64(len(encoded)-lengthBytes) {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: length %d exceeds remaining bytes %d", recordLength, len(encoded)-lengthBytes)
	}
	recordData := encoded[lengthBytes : lengthBytes+int(recordLength)]
	offset := 0
	readVarint := func(field string) (int32, error) {
		value, size, err := decodeSignedVarint(recordData[offset:])
		if err != nil {
			return 0, fmt.Errorf("decode Kafka record v2 %s: %w", field, err)
		}
		offset += size
		return value, nil
	}
	readBytes := func(field string, nullable bool) ([]byte, error) {
		length, err := readVarint(field + " length")
		if err != nil {
			return nil, err
		}
		if length == -1 && nullable {
			return nil, nil
		}
		if length < 0 {
			return nil, fmt.Errorf("decode Kafka record v2 %s: invalid length %d", field, length)
		}
		if int64(length) > int64(len(recordData)-offset) {
			return nil, fmt.Errorf("decode Kafka record v2 %s: length %d exceeds remaining bytes %d", field, length, len(recordData)-offset)
		}
		value := recordData[offset : offset+int(length)]
		offset += int(length)
		return value, nil
	}
	if len(recordData) < 1 {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: missing attributes")
	}
	record := kafkaRecordV2{Attributes: int8(recordData[offset])}
	offset++
	timestampDelta, timestampBytes, err := decodeSignedVarlong(recordData[offset:])
	if err != nil {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2 timestamp delta: %w", err)
	}
	record.TimestampDelta = timestampDelta
	offset += timestampBytes
	record.OffsetDelta, err = readVarint("offset delta")
	if err != nil {
		return kafkaRecordV2{}, 0, err
	}
	record.Key, err = readBytes("key", true)
	if err != nil {
		return kafkaRecordV2{}, 0, err
	}
	record.Value, err = readBytes("value", true)
	if err != nil {
		return kafkaRecordV2{}, 0, err
	}
	headersCount, err := readVarint("headers count")
	if err != nil {
		return kafkaRecordV2{}, 0, err
	}
	if headersCount < 0 {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: negative headers count %d", headersCount)
	}
	if headersCount > int32((len(recordData)-offset)/2) {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: headers count %d exceeds remaining fields", headersCount)
	}
	record.Headers = make([]recordHeaderV2, 0, headersCount)
	for range int(headersCount) {
		key, err := readBytes("header key", false)
		if err != nil {
			return kafkaRecordV2{}, 0, err
		}
		if !utf8.Valid(key) {
			return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2 header key: not valid UTF-8")
		}
		value, err := readBytes("header value", true)
		if err != nil {
			return kafkaRecordV2{}, 0, err
		}
		record.Headers = append(record.Headers, recordHeaderV2{Key: string(key), Value: value})
	}
	if offset != len(recordData) {
		return kafkaRecordV2{}, 0, fmt.Errorf("decode Kafka record v2: %d trailing bytes inside record", len(recordData)-offset)
	}
	return record, lengthBytes + int(recordLength), nil
}
