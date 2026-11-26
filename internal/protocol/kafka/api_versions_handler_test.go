package kafka

import (
	"bytes"
	"testing"
)

func TestHandleAPIVersionsRequestV3(t *testing.T) {
	request := []byte{
		0, 18, 0, 3, 1, 2, 3, 4, // API key, version, correlation ID
		1, 0, // Empty compact client ID and request header tags
		5, 'j', 'a', 'v', 'a', 5, '1', '.', '2', '0', 0, // Body and body tags
	}
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	want := []byte{0, 0, 0, 18, 1, 2, 3, 4, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0}
	got, err := HandleAPIVersionsRequestV3(request, 18, supported)
	if err != nil {
		t.Fatalf("HandleAPIVersionsRequestV3() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("response frame = %x, want %x", got, want)
	}
}

func TestHandleAPIVersionsRequestV3RejectsInvalidRequests(t *testing.T) {
	base := []byte{0, 18, 0, 3, 0, 0, 0, 1, 1, 0, 1, 1, 0}
	tests := []struct {
		name string
		edit func([]byte) []byte
		max  int32
	}{
		{name: "invalid header", edit: func([]byte) []byte { return nil }, max: 64},
		{name: "wrong API key", edit: func(frame []byte) []byte { frame[1] = 17; return frame }, max: 64},
		{name: "wrong API version", edit: func(frame []byte) []byte { frame[3] = 2; return frame }, max: 64},
		{name: "malformed body", edit: func(frame []byte) []byte { return frame[:len(frame)-1] }, max: 64},
		{name: "response exceeds bound", edit: func(frame []byte) []byte { return frame }, max: 17},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := append([]byte(nil), base...)
			if _, err := HandleAPIVersionsRequestV3(test.edit(frame), test.max, []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}); err == nil {
				t.Error("HandleAPIVersionsRequestV3() error = nil, want error")
			}
		})
	}
}
