package kafka

import (
	"bytes"
	"testing"
)

func TestHandleKafkaRequestDispatchesMetadataV0(t *testing.T) {
	request := metadataRequestFrameV0(3, 0, 17, []byte{0, 0, 0, 0})
	response, err := HandleKafkaRequest(request, 1024, nil, nil, nil)
	if err != nil {
		t.Fatalf("HandleKafkaRequest() error = %v", err)
	}
	want := metadataExpectedResponseFrame(17, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	if !bytes.Equal(response, want) {
		t.Errorf("response = %x, want %x", response, want)
	}
}

func TestHandleKafkaRequestDispatchesAPIVersionsV3(t *testing.T) {
	request := []byte{0, 18, 0, 3, 0, 0, 0, 8, 1, 0, 2, 'n', 2, '1', 0}
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	response, err := HandleKafkaRequest(request, 1024, supported, nil, nil)
	if err != nil {
		t.Fatalf("HandleKafkaRequest() error = %v", err)
	}
	want := []byte{0, 0, 0, 18, 0, 0, 0, 8, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0}
	if !bytes.Equal(response, want) {
		t.Errorf("response = %x, want %x", response, want)
	}
}

func TestHandleKafkaRequestRejectsInvalidAndUnsupportedFrames(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame []byte
	}{
		{name: "missing API key", frame: []byte{0}},
		{name: "unsupported API key", frame: []byte{0, 99}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleKafkaRequest(test.frame, 1024, nil, nil, nil); err == nil {
				t.Error("HandleKafkaRequest() error = nil, want error")
			}
		})
	}
}
