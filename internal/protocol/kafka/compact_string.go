package kafka

import (
	"fmt"
	"unicode/utf8"
)

// decodeCompactNullableString reads one Kafka compact nullable UTF-8 string.
func decodeCompactNullableString(encoded []byte) (*string, int, error) {
	length, prefixLength, err := decodeUnsignedVarint(encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("decode Kafka compact nullable string length: %w", err)
	}
	if length == 0 {
		return nil, prefixLength, nil
	}

	stringLength := uint64(length - 1)
	remaining := uint64(len(encoded) - prefixLength)
	if stringLength > remaining {
		return nil, 0, fmt.Errorf("decode Kafka compact nullable string: length %d exceeds remaining bytes %d", stringLength, remaining)
	}
	stringEnd := prefixLength + int(stringLength)
	stringBytes := encoded[prefixLength:stringEnd]
	if !utf8.Valid(stringBytes) {
		return nil, 0, fmt.Errorf("decode Kafka compact nullable string: invalid UTF-8")
	}
	value := string(stringBytes)
	return &value, stringEnd, nil
}
