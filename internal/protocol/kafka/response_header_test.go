package kafka

import (
	"bytes"
	"testing"
)

func TestEncodeResponseHeaderV0(t *testing.T) {
	tests := []struct {
		name          string
		correlationID int32
		want          []byte
	}{
		{name: "zero", correlationID: 0, want: []byte{0, 0, 0, 0}},
		{name: "positive", correlationID: 0x01020304, want: []byte{1, 2, 3, 4}},
		{name: "negative", correlationID: -1, want: []byte{0xff, 0xff, 0xff, 0xff}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := encodeResponseHeaderV0(test.correlationID)
			if !bytes.Equal(got, test.want) {
				t.Errorf("encodeResponseHeaderV0(%d) = %x, want %x", test.correlationID, got, test.want)
			}
			if len(got) != 4 {
				t.Errorf("response header length = %d, want 4", len(got))
			}
		})
	}
}
