package kafka

import (
	"reflect"
	"testing"
)

func TestDecodeMetadataRequestV0(t *testing.T) {
	tests := []struct {
		name  string
		body  []byte
		want  []string
		valid bool
	}{
		{name: "empty topics", body: []byte{0, 0, 0, 0}, want: []string{}, valid: true},
		{name: "multiple UTF-8 topics", body: []byte{0, 0, 0, 2, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 3, 0xe7, 0x8c, 0xab}, want: []string{"orders", "猫"}, valid: true},
		{name: "truncated array count", body: []byte{0, 0}, valid: false},
		{name: "negative topic count", body: []byte{0xff, 0xff, 0xff, 0xff}, valid: false},
		{name: "count exceeds available fields", body: []byte{0, 0, 0, 1}, valid: false},
		{name: "negative topic length", body: []byte{0, 0, 0, 1, 0xff, 0xff}, valid: false},
		{name: "truncated topic bytes", body: []byte{0, 0, 0, 1, 0, 3, 'x'}, valid: false},
		{name: "invalid UTF-8", body: []byte{0, 0, 0, 1, 0, 1, 0xff}, valid: false},
		{name: "trailing bytes", body: []byte{0, 0, 0, 0, 0xff}, valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeMetadataRequestV0(test.body)
			if (err == nil) != test.valid {
				t.Fatalf("decodeMetadataRequestV0() error = %v, valid = %v", err, test.valid)
			}
			if test.valid && !reflect.DeepEqual(got, test.want) {
				t.Errorf("topics = %v, want %v", got, test.want)
			}
		})
	}
}
