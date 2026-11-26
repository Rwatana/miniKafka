package kafka

import "fmt"

// skipTaggedFields validates and skips one Kafka flexible tagged-field section.
func skipTaggedFields(encoded []byte) (int, error) {
	count, cursor, err := decodeUnsignedVarint(encoded)
	if err != nil {
		return 0, fmt.Errorf("read Kafka tagged-field count: %w", err)
	}
	var previousTag uint32
	for index := uint32(0); index < count; index++ {
		tag, tagBytes, err := decodeUnsignedVarint(encoded[cursor:])
		if err != nil {
			return 0, fmt.Errorf("read Kafka tagged-field tag %d: %w", index, err)
		}
		cursor += tagBytes
		if index > 0 && tag <= previousTag {
			return 0, fmt.Errorf("read Kafka tagged-field tag %d: tags must be strictly increasing", index)
		}
		previousTag = tag

		fieldSize, sizeBytes, err := decodeUnsignedVarint(encoded[cursor:])
		if err != nil {
			return 0, fmt.Errorf("read Kafka tagged-field size for tag %d: %w", tag, err)
		}
		cursor += sizeBytes
		remaining := len(encoded) - cursor
		if uint64(fieldSize) > uint64(remaining) {
			return 0, fmt.Errorf("read Kafka tagged-field data for tag %d: size %d exceeds remaining bytes %d", tag, fieldSize, remaining)
		}
		cursor += int(fieldSize)
	}
	return cursor, nil
}
