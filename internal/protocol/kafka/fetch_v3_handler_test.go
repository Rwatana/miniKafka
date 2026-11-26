package kafka

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
)

func TestHandleFetchRequestV3HonorsAggregateBudget(t *testing.T) {
	request := fetchRequestFrameV3ForTest(fetchRequestV3BodyForTest(fetchRequestV3{MaxBytes: 3, Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0, PartitionMaxBytes: 10}, {Index: 1, PartitionMaxBytes: 10}}}}}))
	var limits []int32
	response, err := HandleFetchRequestV3(request, 1024, func(_ string, partition int, _ int64, maxBytes int32) ([]byte, int64, error) {
		limits = append(limits, maxBytes)
		if partition == 0 {
			return []byte{1, 2, 3, 4}, 2, nil
		}
		return []byte{3, 4}, 2, nil
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV3() error = %v", err)
	}
	if !reflect.DeepEqual(limits, []int32{3, 0}) {
		t.Errorf("reader byte limits = %v, want [3 0]", limits)
	}
	wantBody, err := encodeFetchResponseV2(fetchResponseV2{Topics: []fetchTopicResponseV2{{Name: "orders", Partitions: []fetchPartitionResponseV2{{Index: 0, HighWatermark: 2, Records: []byte{1, 2, 3, 4}}, {Index: 1, HighWatermark: 2, Records: nil}}}}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("Fetch v3 response = %x, want %x", response, want)
	}
}

func TestHandleFetchRequestV3MapsPartitionErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		readErr  error
		high     int64
		wantCode int16
		wantHigh int64
	}{
		{name: "storage error", readErr: errors.New("disk read failed"), wantCode: -1, wantHigh: -1},
		{name: "missing partition", readErr: kafkaerr.ErrUnknownTopicOrPartition, wantCode: 3, wantHigh: -1},
		{name: "offset beyond high watermark", high: -1, wantCode: 1, wantHigh: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := fetchRequestFrameV3ForTest(fetchRequestV3BodyForTest(fetchRequestV3{Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0}}}}}))
			response, err := HandleFetchRequestV3(request, 1024, func(string, int, int64, int32) ([]byte, int64, error) { return nil, test.high, test.readErr })
			if err != nil {
				t.Fatalf("HandleFetchRequestV3() error = %v", err)
			}
			wantBody, err := encodeFetchResponseV2(fetchResponseV2{Topics: []fetchTopicResponseV2{{Name: "orders", Partitions: []fetchPartitionResponseV2{{Index: 0, ErrorCode: test.wantCode, HighWatermark: test.wantHigh}}}}})
			if err != nil {
				t.Fatal(err)
			}
			want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(response, want) {
				t.Errorf("Fetch v3 response = %x, want %x", response, want)
			}
		})
	}
}

func TestHandleFetchRequestV3WaitsForMinBytes(t *testing.T) {
	request := fetchRequestFrameV3ForTest(fetchRequestV3BodyForTest(fetchRequestV3{MaxWaitMs: 1, MinBytes: 2, MaxBytes: 10, Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0, PartitionMaxBytes: 10}}}}}))
	calls := 0
	_, err := HandleFetchRequestV3(request, 1024, func(string, int, int64, int32) ([]byte, int64, error) {
		calls++
		if calls == 1 {
			return []byte{}, 0, nil
		}
		return []byte{1, 2}, 2, nil
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV3() error = %v", err)
	}
	if calls < 2 {
		t.Errorf("reader calls = %d, want retry until min_bytes", calls)
	}
}

func TestHandleFetchRequestV3RejectsInvalidRequestsAndResponses(t *testing.T) {
	valid := fetchRequestFrameV3ForTest(fetchRequestV3BodyForTest(fetchRequestV3{}))
	wrongKey := append([]byte(nil), valid...)
	wrongKey[1] = 2
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[3] = 2
	malformedBody := append([]byte(nil), valid[:len(valid)-1]...)
	for _, test := range []struct {
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
		{name: "response encoding error", request: fetchRequestFrameV3ForTest(fetchRequestV3BodyForTest(fetchRequestV3{Topics: []fetchTopicRequestV4{{Name: "orders"}}})), reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1024},
		{name: "response frame too large", request: valid, reader: func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }, max: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "response encoding error" {
				original := encodeFetchV3HandlerResponseBody
				encodeFetchV3HandlerResponseBody = func(fetchResponseV2) ([]byte, error) { return nil, errors.New("encode failed") }
				defer func() { encodeFetchV3HandlerResponseBody = original }()
			}
			if _, err := HandleFetchRequestV3(test.request, test.max, test.reader); err == nil {
				t.Error("HandleFetchRequestV3() error = nil, want error")
			}
		})
	}
}

func fetchRequestFrameV3ForTest(body []byte) []byte {
	frame := []byte{0, 1, 0, 3, 0, 0, 0, 42, 0xff, 0xff}
	return append(frame, body...)
}
