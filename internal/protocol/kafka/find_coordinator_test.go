package kafka

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestHandleFindCoordinatorRequestV1(t *testing.T) {
	request := findCoordinatorRequestFrameV1ForTest([]byte{0, 6, 'g', 'r', 'o', 'u', 'p', '1', 0})
	got, err := HandleFindCoordinatorRequestV1(request, 1024, MetadataBroker{NodeID: 3, Host: "broker.internal", Port: 9092})
	if err != nil {
		t.Fatalf("HandleFindCoordinatorRequestV1() error = %v", err)
	}
	wantBody := []byte{
		0, 0, 0, 0,
		0, 0,
		0xff, 0xff,
		0, 0, 0, 3,
		0, 15, 'b', 'r', 'o', 'k', 'e', 'r', '.', 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l',
		0, 0, 0x23, 0x84,
	}
	want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("FindCoordinator response = %x, want %x", got, want)
	}
}

func TestHandleFindCoordinatorRequestV1RejectsInvalidRequests(t *testing.T) {
	valid := findCoordinatorRequestFrameV1ForTest([]byte{0, 1, 'g', 0})
	wrongKey := append([]byte(nil), valid...)
	wrongKey[1] = 3
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[3] = 0
	tests := []struct {
		name        string
		request     []byte
		coordinator MetadataBroker
		maxBytes    int32
	}{
		{name: "short header", request: []byte{0}, coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "wrong API key", request: wrongKey, coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "wrong API version", request: wrongVersion, coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "short body", request: findCoordinatorRequestFrameV1ForTest([]byte{0, 1}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "negative key length", request: findCoordinatorRequestFrameV1ForTest([]byte{0xff, 0xff, 0}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "key exceeds body", request: findCoordinatorRequestFrameV1ForTest([]byte{0, 3, 'g', 0}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "invalid key UTF-8", request: findCoordinatorRequestFrameV1ForTest([]byte{0, 1, 0xff, 0}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "unsupported coordinator type", request: findCoordinatorRequestFrameV1ForTest([]byte{0, 1, 'g', 1}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "trailing bytes", request: findCoordinatorRequestFrameV1ForTest([]byte{0, 1, 'g', 0, 1}), coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "invalid endpoint", request: valid, coordinator: MetadataBroker{NodeID: -1, Host: "localhost", Port: 9092}, maxBytes: 1024},
		{name: "response too large", request: valid, coordinator: MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, maxBytes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleFindCoordinatorRequestV1(test.request, test.maxBytes, test.coordinator); err == nil {
				t.Error("HandleFindCoordinatorRequestV1() error = nil, want error")
			}
		})
	}
}

func findCoordinatorRequestFrameV1ForTest(body []byte) []byte {
	frame := make([]byte, 10, 10+len(body))
	binary.BigEndian.PutUint16(frame[0:2], 10)
	binary.BigEndian.PutUint16(frame[2:4], 1)
	binary.BigEndian.PutUint32(frame[4:8], 42)
	binary.BigEndian.PutUint16(frame[8:10], 0xffff)
	return append(frame, body...)
}
