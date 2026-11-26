package kafka

import (
	"bytes"
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
