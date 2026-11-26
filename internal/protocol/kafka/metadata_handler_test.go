package kafka

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestHandleMetadataRequestV0(t *testing.T) {
	broker := MetadataBroker{NodeID: 7, Host: "b", Port: 9092}
	topic := MetadataTopic{Name: "orders"}
	request := metadataRequestFrameV0(3, 0, 42, []byte{0, 0, 0, 2, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 4, 'm', 'i', 's', 's'})
	response, err := HandleMetadataRequestV0(request, 1024, []MetadataBroker{broker}, []MetadataTopic{topic})
	if err != nil {
		t.Fatalf("HandleMetadataRequestV0() error = %v", err)
	}
	wantBody := []byte{
		0, 0, 0, 1, 0, 0, 0, 7, 0, 1, 'b', 0, 0, 0x23, 0x84,
		0, 0, 0, 2, 0, 0, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 0,
		0, 3, 0, 4, 'm', 'i', 's', 's', 0, 0, 0, 0,
	}
	wantFrame := metadataExpectedResponseFrame(42, wantBody)
	if !bytes.Equal(response, wantFrame) {
		t.Errorf("response = %x, want %x", response, wantFrame)
	}
}

func TestHandleMetadataRequestV0AllTopics(t *testing.T) {
	topic := MetadataTopic{Name: "orders"}
	response, err := HandleMetadataRequestV0(metadataRequestFrameV0(3, 0, 9, []byte{0, 0, 0, 0}), 1024, nil, []MetadataTopic{topic})
	if err != nil {
		t.Fatalf("HandleMetadataRequestV0() error = %v", err)
	}
	if got, want := int32(binary.BigEndian.Uint32(response[4:8])), int32(9); got != want {
		t.Errorf("correlation ID = %d, want %d", got, want)
	}
	wantBody := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 0, 0, 0}
	wantFrame := metadataExpectedResponseFrame(9, wantBody)
	if !bytes.Equal(response, wantFrame) {
		t.Errorf("response = %x, want %x", response, wantFrame)
	}
}

func TestHandleMetadataRequestV0RejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "truncated header", frame: []byte{0}},
		{name: "wrong API key", frame: metadataRequestFrameV0(18, 0, 1, []byte{0, 0, 0, 0})},
		{name: "wrong API version", frame: metadataRequestFrameV0(3, 1, 1, []byte{0, 0, 0, 0})},
		{name: "malformed body", frame: metadataRequestFrameV0(3, 0, 1, []byte{0, 0})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleMetadataRequestV0(test.frame, 1024, nil, nil); err == nil {
				t.Error("HandleMetadataRequestV0() error = nil, want error")
			}
		})
	}
}

func TestHandleMetadataRequestV0RejectsInvalidResponse(t *testing.T) {
	t.Run("invalid metadata string", func(t *testing.T) {
		frame := metadataRequestFrameV0(3, 0, 1, []byte{0, 0, 0, 0})
		if _, err := HandleMetadataRequestV0(frame, 1024, []MetadataBroker{{Host: "\xff"}}, nil); err == nil {
			t.Error("HandleMetadataRequestV0() error = nil, want metadata encoding error")
		}
	})
	t.Run("response frame limit", func(t *testing.T) {
		frame := metadataRequestFrameV0(3, 0, 1, []byte{0, 0, 0, 0})
		if _, err := HandleMetadataRequestV0(frame, 1, nil, nil); err == nil {
			t.Error("HandleMetadataRequestV0() error = nil, want frame size error")
		}
	})
}

func TestHandleMetadataRequestV1(t *testing.T) {
	broker := MetadataBroker{NodeID: 7, Host: "broker", Port: 9092}
	topic := MetadataTopic{Name: "orders", Partitions: []MetadataPartition{{PartitionIndex: 0, LeaderID: 7, ReplicaNodes: []int32{7}, ISRNodes: []int32{7}}}}
	request := metadataRequestFrameV0(3, 1, 42, []byte{0xff, 0xff, 0xff, 0xff})
	response, err := HandleMetadataRequestV1(request, 1024, []MetadataBroker{broker}, []MetadataTopic{topic})
	if err != nil {
		t.Fatalf("HandleMetadataRequestV1() error = %v", err)
	}
	responseBody, err := encodeMetadataResponseV1([]MetadataBroker{broker}, 7, []MetadataTopic{topic})
	if err != nil {
		t.Fatal(err)
	}
	if want := metadataExpectedResponseFrame(42, responseBody); !bytes.Equal(response, want) {
		t.Errorf("response = %x, want %x", response, want)
	}

	specific := metadataRequestFrameV0(3, 1, 17, []byte{0, 0, 0, 1, 0, 6, 'o', 'r', 'd', 'e', 'r', 's'})
	response, err = HandleMetadataRequestV1(specific, 1024, []MetadataBroker{broker}, []MetadataTopic{topic})
	if err != nil {
		t.Fatalf("HandleMetadataRequestV1(specific) error = %v", err)
	}
	if got, want := int32(binary.BigEndian.Uint32(response[4:8])), int32(17); got != want {
		t.Errorf("correlation ID = %d, want %d", got, want)
	}
	missing := metadataRequestFrameV0(3, 1, 18, []byte{0, 0, 0, 1, 0, 4, 'm', 'i', 's', 's'})
	response, err = HandleMetadataRequestV1(missing, 1024, []MetadataBroker{broker}, []MetadataTopic{topic})
	if err != nil {
		t.Fatalf("HandleMetadataRequestV1(missing topic) error = %v", err)
	}
	missingBody, err := encodeMetadataResponseV1([]MetadataBroker{broker}, 7, []MetadataTopic{{ErrorCode: 3, Name: "miss"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := metadataExpectedResponseFrame(18, missingBody); !bytes.Equal(response, want) {
		t.Errorf("missing topic response = %x, want %x", response, want)
	}

	for _, test := range []struct {
		name  string
		frame []byte
		max   int32
		broks []MetadataBroker
	}{
		{name: "truncated header", frame: []byte{0}, max: 1024},
		{name: "wrong API key", frame: metadataRequestFrameV0(18, 1, 1, []byte{0, 0, 0, 0}), max: 1024},
		{name: "wrong API version", frame: metadataRequestFrameV0(3, 2, 1, []byte{0, 0, 0, 0}), max: 1024},
		{name: "malformed body", frame: metadataRequestFrameV0(3, 1, 1, []byte{0, 0}), max: 1024},
		{name: "invalid response string", frame: request, max: 1024, broks: []MetadataBroker{{Host: "\xff"}}},
		{name: "response frame limit", frame: request, max: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleMetadataRequestV1(test.frame, test.max, test.broks, nil); err == nil {
				t.Error("HandleMetadataRequestV1() error = nil, want error")
			}
		})
	}
}

func metadataRequestFrameV0(apiKey, apiVersion int16, correlationID int32, body []byte) []byte {
	frame := make([]byte, 10, 10+len(body))
	binary.BigEndian.PutUint16(frame[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(frame[2:4], uint16(apiVersion))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	binary.BigEndian.PutUint16(frame[8:10], 0xffff)
	return append(frame, body...)
}

func metadataExpectedResponseFrame(correlationID int32, body []byte) []byte {
	frame := make([]byte, 4, 8+len(body))
	binary.BigEndian.PutUint32(frame, uint32(4+len(body)))
	var correlation [4]byte
	binary.BigEndian.PutUint32(correlation[:], uint32(correlationID))
	frame = append(frame, correlation[:]...)
	return append(frame, body...)
}
