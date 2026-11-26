package kafka

import (
	"bytes"
	"testing"
)

func TestEncodeAPIVersionsResponseV3(t *testing.T) {
	tests := []struct {
		name        string
		errorCode   int16
		apiVersions []apiVersionRange
		want        []byte
	}{
		{
			name: "multiple API ranges",
			apiVersions: []apiVersionRange{
				{apiKey: 18, minVersion: 0, maxVersion: 5},
				{apiKey: 0, minVersion: 1, maxVersion: 4},
			},
			want: []byte{0, 0, 3, 0, 18, 0, 0, 0, 5, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0},
		},
		{name: "empty API list with error", errorCode: -1, apiVersions: []apiVersionRange{}, want: []byte{0xff, 0xff, 1, 0, 0, 0, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := encodeAPIVersionsResponseV3(test.errorCode, test.apiVersions)
			if !bytes.Equal(got, test.want) {
				t.Errorf("response = %x, want %x", got, test.want)
			}
		})
	}
}

func TestEncodeAPIVersionsResponseFrameV3(t *testing.T) {
	apiVersions := []apiVersionRange{{apiKey: 18, minVersion: 0, maxVersion: 3}}
	want := []byte{0, 0, 0, 18, 1, 2, 3, 4, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0}
	got, err := encodeAPIVersionsResponseFrameV3(0x01020304, 0, apiVersions, 18)
	if err != nil {
		t.Fatalf("encodeAPIVersionsResponseFrameV3() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("response frame = %x, want %x", got, want)
	}
	if _, err := encodeAPIVersionsResponseFrameV3(0x01020304, 0, apiVersions, 17); err == nil {
		t.Error("encodeAPIVersionsResponseFrameV3(max=17) error = nil, want oversized-frame error")
	}
}
