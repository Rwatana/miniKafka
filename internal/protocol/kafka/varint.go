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
