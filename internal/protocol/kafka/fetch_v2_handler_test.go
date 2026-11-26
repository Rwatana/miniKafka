package kafka

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
)

func TestHandleFetchRequestV2ReadsBatchesAndFramesResponse(t *testing.T) {
	request := fetchRequestFrameV2ForTest(fetchRequestV2BodyForTest(fetchRequestV2{
		ReplicaID: -1, MaxWaitMs: 20, MinBytes: 1,
		Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 2, FetchOffset: 7, PartitionMaxBytes: 128}}}},
	}))
	called := false
	response, err := HandleFetchRequestV2(request, 1024, func(topic string, partition int, offset int64, maxBytes int32) ([]byte, int64, error) {
		called = true
		if topic != "orders" || partition != 2 || offset != 7 || maxBytes != 128 {
			t.Errorf("reader args = (%q, %d, %d, %d), want (orders, 2, 7, 128)", topic, partition, offset, maxBytes)
		}
		return []byte{0xaa, 0xbb}, 8, nil
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV2() error = %v", err)
	}
	if !called {
		t.Fatal("Kafka batch reader was not called")
	}
	wantBody, err := encodeFetchResponseV2(fetchResponseV2{Topics: []fetchTopicResponseV2{{Name: "orders", Partitions: []fetchPartitionResponseV2{{Index: 2, HighWatermark: 8, Records: []byte{0xaa, 0xbb}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("Fetch v2 response = %x, want %x", response, want)
	}
}

func TestHandleFetchRequestV2MapsPartitionErrors(t *testing.T) {
	request := fetchRequestFrameV2ForTest(fetchRequestV2BodyForTest(fetchRequestV2{Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0}}}}}))
	for _, test := range []struct {
		name        string
		readErr     error
		high        int64
		wantCode    int16
		wantHigh    int64
		wantRecords []byte
	}{
		{name: "storage error", readErr: errors.New("disk read failed"), wantCode: -1, wantHigh: -1},
		{name: "missing partition", readErr: kafkaerr.ErrUnknownTopicOrPartition, wantCode: 3, wantHigh: -1},
		{name: "offset beyond high watermark", high: -1, wantCode: 1, wantHigh: -1},
		{name: "valid empty batch", high: 0, wantCode: 0, wantHigh: 0, wantRecords: []byte{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, err := HandleFetchRequestV2(request, 1024, func(string, int, int64, int32) ([]byte, int64, error) {
				return test.wantRecords, test.high, test.readErr
			})
			if err != nil {
				t.Fatalf("HandleFetchRequestV2() error = %v", err)
			}
			wantBody, err := encodeFetchResponseV2(fetchResponseV2{Topics: []fetchTopicResponseV2{{Name: "orders", Partitions: []fetchPartitionResponseV2{{Index: 0, ErrorCode: test.wantCode, HighWatermark: test.wantHigh, Records: test.wantRecords}}}}})
			if err != nil {
				t.Fatal(err)
			}
			want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(response, want) {
				t.Errorf("Fetch v2 response = %x, want %x", response, want)
			}
		})
	}
}

func TestHandleFetchRequestV2WaitsForMinBytes(t *testing.T) {
	request := fetchRequestFrameV2ForTest(fetchRequestV2BodyForTest(fetchRequestV2{MaxWaitMs: 1, MinBytes: 2, Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0, PartitionMaxBytes: 10}}}}}))
	calls := 0
	_, err := HandleFetchRequestV2(request, 1024, func(string, int, int64, int32) ([]byte, int64, error) {
		calls++
		if calls == 1 {
			return []byte{}, 0, nil
		}
		return []byte{1, 2}, 2, nil
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV2() error = %v", err)
	}
	if calls < 2 {
		t.Errorf("reader calls = %d, want retry until min_bytes", calls)
	}
}

func TestHandleFetchRequestV2RejectsInvalidRequestsAndResponses(t *testing.T) {
	valid := fetchRequestFrameV2ForTest(fetchRequestV2BodyForTest(fetchRequestV2{}))
	wrongKey := append([]byte(nil), valid...)
	wrongKey[1] = 2
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[3] = 3
	malformedBody := append([]byte(nil), valid[:len(valid)-1]...)
	tests := []struct {
		name    string
		request []byte
		reader  KafkaBatchReader
		max     int32
	}{
		{name: "short header", request: []byte{0}, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "wrong API key", request: wrongKey, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "wrong API version", request: wrongVersion, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "missing reader", request: valid, max: 1024},
		{name: "malformed body", request: malformedBody, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "response encoding error", request: fetchRequestFrameV2ForTest(fetchRequestV2BodyForTest(fetchRequestV2{Topics: []fetchTopicRequestV4{{Name: "orders"}}})), reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "response frame too large", request: valid, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "response encoding error" {
				original := encodeFetchV2HandlerResponseBody
				encodeFetchV2HandlerResponseBody = func(fetchResponseV2) ([]byte, error) { return nil, errors.New("encode failed") }
				defer func() { encodeFetchV2HandlerResponseBody = original }()
			}
			if _, err := HandleFetchRequestV2(test.request, test.max, test.reader); err == nil {
				t.Error("HandleFetchRequestV2() error = nil, want error")
			}
		})
	}
}

func fetchRequestFrameV2ForTest(body []byte) []byte {
	frame := []byte{0, 1, 0, 2, 0, 0, 0, 42, 0xff, 0xff}
	frame = append(frame, body...)
	return frame
}
