package kafka

import "testing"

func TestSkipTaggedFields(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		consumed int
		rest     byte
	}{
		{name: "empty fields", input: []byte{0, 0xff}, consumed: 1, rest: 0xff},
		{name: "one unknown field", input: []byte{1, 2, 2, 0xaa, 0xbb, 0xff}, consumed: 5, rest: 0xff},
		{name: "multiple sorted fields", input: []byte{2, 1, 1, 0xaa, 3, 0, 0xff}, consumed: 6, rest: 0xff},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumed, err := skipTaggedFields(test.input)
			if err != nil {
				t.Fatalf("skipTaggedFields() error = %v", err)
			}
			if consumed != test.consumed || test.input[consumed] != test.rest {
				t.Errorf("consumed = %d, next byte = %x; want %d, %x", consumed, test.input[consumed], test.consumed, test.rest)
			}
		})
	}
}

func TestSkipTaggedFieldsRejectsMalformedInput(t *testing.T) {
	for name, input := range map[string][]byte{
		"truncated count":      {0x80},
		"missing tag":          {1},
		"truncated tag":        {1, 0x80},
		"missing size":         {1, 0},
		"truncated size":       {1, 0, 0x80},
		"truncated field data": {1, 0, 2, 0xaa},
		"duplicate tags":       {2, 1, 0, 1, 0},
		"descending tags":      {2, 2, 0, 1, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := skipTaggedFields(input); err == nil {
				t.Error("skipTaggedFields() error = nil, want malformed-field error")
			}
		})
	}
}
