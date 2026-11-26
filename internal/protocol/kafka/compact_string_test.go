package kafka

import (
	"bytes"
	"testing"
)

func TestDecodeCompactNullableString(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		wantString *string
		wantBytes  int
		wantRest   []byte
	}{
		{name: "null", input: []byte{0, 0xff}, wantBytes: 1, wantRest: []byte{0xff}},
		{name: "empty", input: []byte{1, 0xff}, wantString: stringPointer(""), wantBytes: 1, wantRest: []byte{0xff}},
		{name: "ascii with remainder", input: []byte{3, 'g', 'o', 0xff}, wantString: stringPointer("go"), wantBytes: 3, wantRest: []byte{0xff}},
		{name: "multibyte UTF-8", input: []byte{4, 0xe7, 0x8c, 0xab}, wantString: stringPointer("猫"), wantBytes: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, consumed, err := decodeCompactNullableString(test.input)
			if err != nil {
				t.Fatalf("decodeCompactNullableString() error = %v", err)
			}
			if (got == nil) != (test.wantString == nil) || got != nil && *got != *test.wantString {
				t.Errorf("decoded string = %v, want %v", got, test.wantString)
			}
			if consumed != test.wantBytes || !bytes.Equal(test.input[consumed:], test.wantRest) {
				t.Errorf("consumed = %d, remainder = %v; want %d, %v", consumed, test.input[consumed:], test.wantBytes, test.wantRest)
			}
		})
	}
}

func TestDecodeCompactNullableStringRejectsInvalidInput(t *testing.T) {
	for name, input := range map[string][]byte{
		"truncated varint": {0x80},
		"truncated string": {4, 'x'},
		"invalid UTF-8":    {2, 0xff},
		"oversized length": {0xff, 0xff, 0xff, 0xff, 0x0f},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeCompactNullableString(input); err == nil {
				t.Error("decodeCompactNullableString() error = nil, want error")
			}
		})
	}
}

func TestDecodeCompactString(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		want      string
		consumed  int
		wantError bool
	}{
		{name: "empty", input: []byte{1, 0xff}, want: "", consumed: 1},
		{name: "UTF-8 with remainder", input: []byte{4, 0xe7, 0x8c, 0xab, 0xff}, want: "猫", consumed: 4},
		{name: "rejects null", input: []byte{0}, wantError: true},
		{name: "rejects malformed length", input: []byte{0x80}, wantError: true},
		{name: "rejects invalid UTF-8", input: []byte{2, 0xff}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, consumed, err := decodeCompactString(test.input)
			if (err != nil) != test.wantError {
				t.Fatalf("decodeCompactString() error = %v, wantError %v", err, test.wantError)
			}
			if test.wantError {
				return
			}
			if got != test.want {
				t.Errorf("decoded string = %q, want %q", got, test.want)
			}
			if consumed != test.consumed {
				t.Errorf("consumed = %d, want %d", consumed, test.consumed)
			}
			if !bytes.Equal(test.input[consumed:], []byte{0xff}) && test.name == "UTF-8 with remainder" {
				t.Errorf("remainder = %x, want ff", test.input[consumed:])
			}
		})
	}
}

func stringPointer(value string) *string {
	return &value
}
