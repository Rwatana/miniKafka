package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestDecodeUnsignedVarint(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		value  uint32
		length int
		rest   []byte
	}{
		{name: "zero", input: []byte{0}, value: 0, length: 1},
		{name: "single byte", input: []byte{0x7f}, value: 127, length: 1},
		{name: "two bytes", input: []byte{0xac, 0x02, 0xff}, value: 300, length: 2, rest: []byte{0xff}},
		{name: "three bytes", input: []byte{0x80, 0x80, 0x01}, value: 16384, length: 3},
		{name: "maximum uint32", input: []byte{0xff, 0xff, 0xff, 0xff, 0x0f}, value: ^uint32(0), length: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, length, err := decodeUnsignedVarint(test.input)
			if err != nil {
				t.Fatalf("decodeUnsignedVarint() error = %v", err)
			}
			if value != test.value || length != test.length {
				t.Errorf("decodeUnsignedVarint() = (%d, %d), want (%d, %d)", value, length, test.value, test.length)
			}
			if !bytes.Equal(test.input[length:], test.rest) {
				t.Errorf("bytes after varint = %v, want %v", test.input[length:], test.rest)
			}
		})
	}
}

func TestDecodeUnsignedVarintRejectsInvalidInput(t *testing.T) {
	for name, input := range map[string][]byte{
		"empty":                 nil,
		"truncated":             {0x80},
		"too many continuation": {0x80, 0x80, 0x80, 0x80, 0x80},
		"overflow":              {0xff, 0xff, 0xff, 0xff, 0x10},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeUnsignedVarint(input); err == nil {
				t.Error("decodeUnsignedVarint() error = nil, want error")
			}
		})
	}
}

func TestDecodeSignedVarint(t *testing.T) {
	for _, value := range []int32{-1 << 31, -65536, -64, -1, 0, 1, 63, 64, 65536, 1<<31 - 1} {
		t.Run(fmt.Sprintf("%d", value), func(t *testing.T) {
			encoded := signedVarintForTest(value)
			got, length, err := decodeSignedVarint(append(encoded, 0xff))
			if err != nil {
				t.Fatalf("decodeSignedVarint() error = %v", err)
			}
			if got != value || length != len(encoded) {
				t.Errorf("decodeSignedVarint() = (%d, %d), want (%d, %d)", got, length, value, len(encoded))
			}
		})
	}
}

func TestDecodeSignedVarintRejectsInvalidEncoding(t *testing.T) {
	for name, encoded := range map[string][]byte{
		"truncated": {0x80},
		"overflow":  {0xff, 0xff, 0xff, 0xff, 0x10},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeSignedVarint(encoded); err == nil {
				t.Error("decodeSignedVarint() error = nil, want error")
			}
		})
	}
}

func signedVarintForTest(value int32) []byte {
	zigzag := uint32(value<<1) ^ uint32(value>>31)
	var encoded [binary.MaxVarintLen32]byte
	length := binary.PutUvarint(encoded[:], uint64(zigzag))
	return append([]byte(nil), encoded[:length]...)
}
