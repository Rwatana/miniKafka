package kafka

import "fmt"

// decodeUnsignedVarint reads a Kafka 32-bit unsigned base-128 integer.
func decodeUnsignedVarint(encoded []byte) (uint32, int, error) {
	var value uint32
	for index := 0; index < 5; index++ {
		if index >= len(encoded) {
			return 0, 0, fmt.Errorf("decode Kafka unsigned varint: truncated after %d bytes", index)
		}
		current := encoded[index]
		if index == 4 && current&0x7f > 0x0f {
			return 0, 0, fmt.Errorf("decode Kafka unsigned varint: value exceeds 32 bits")
		}
		value |= uint32(current&0x7f) << (7 * index)
		if current&0x80 == 0 {
			return value, index + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("decode Kafka unsigned varint: exceeds five bytes")
}

// decodeSignedVarint decodes a Kafka signed zig-zag VARINT and its encoded length.
func decodeSignedVarint(encoded []byte) (int32, int, error) {
	unsigned, length, err := decodeUnsignedVarint(encoded)
	if err != nil {
		return 0, 0, fmt.Errorf("decode Kafka signed varint: %w", err)
	}
	value := int32(unsigned>>1) ^ -int32(unsigned&1)
	return value, length, nil
}

// decodeUnsignedVarlong decodes a Kafka 64-bit unsigned base-128 integer.
func decodeUnsignedVarlong(encoded []byte) (uint64, int, error) {
	var value uint64
	for index := 0; index < 10 && index < len(encoded); index++ {
		current := encoded[index]
		if index == 9 && current&0x7e != 0 {
			return 0, 0, fmt.Errorf("decode Kafka unsigned varlong: value exceeds 64 bits")
		}
		value |= uint64(current&0x7f) << (7 * index)
		if current&0x80 == 0 {
			return value, index + 1, nil
		}
	}
	if len(encoded) >= 10 {
		return 0, 0, fmt.Errorf("decode Kafka unsigned varlong: exceeds ten bytes")
	}
	return 0, 0, fmt.Errorf("decode Kafka unsigned varlong: truncated after %d bytes", len(encoded))
}
