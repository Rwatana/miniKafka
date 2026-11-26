package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestHandleFetchRequestV4ReadsBatchesAndFramesResponse(t *testing.T) {
	body := fetchRequestV4BodyForTest(fetchRequestV4{
		ReplicaID: -1, MaxWaitMs: 250, MinBytes: 1, MaxBytes: 1024,
		Topics: []fetchTopicRequestV4{
			{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0, FetchOffset: 7, PartitionMaxBytes: 128}, {Index: 2, FetchOffset: 9, PartitionMaxBytes: 256}}},
			{Name: "events", Partitions: []fetchPartitionRequestV4{}},
		},
	})
	request := fetchRequestFrameV4ForTest(body)
	type readCall struct {
		topic     string
		partition int
		offset    int64
		maxBytes  int32
	}
	var calls []readCall
	response, err := HandleFetchRequestV4(request, 1024, func(topic string, partition int, offset int64, maxBytes int32) ([]byte, int64, error) {
		calls = append(calls, readCall{topic: topic, partition: partition, offset: offset, maxBytes: maxBytes})
		return []byte{byte(partition), 0xaa}, offset + 3, nil
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV4() error = %v", err)
	}
	wantCalls := []readCall{{topic: "orders", partition: 0, offset: 7, maxBytes: 128}, {topic: "orders", partition: 2, offset: 9, maxBytes: 256}}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Errorf("reader calls = %+v, want %+v", calls, wantCalls)
	}
	wantBody, err := encodeFetchResponseV4(fetchResponseV4{Topics: []fetchTopicResponseV4{
		{Name: "orders", Partitions: []fetchPartitionResponseV4{
			{Index: 0, HighWatermark: 10, LastStableOffset: 10, Records: []byte{0, 0xaa}},
			{Index: 2, HighWatermark: 12, LastStableOffset: 12, Records: []byte{2, 0xaa}},
		}},
		{Name: "events", Partitions: []fetchPartitionResponseV4{}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("Fetch response = %x, want %x", response, want)
	}
}

func TestHandleFetchRequestV4MapsPartitionReadErrors(t *testing.T) {
	body := fetchRequestV4BodyForTest(fetchRequestV4{Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 1}}}}})
	response, err := HandleFetchRequestV4(fetchRequestFrameV4ForTest(body), 1024, func(string, int, int64, int32) ([]byte, int64, error) {
		return nil, 0, errors.New("disk read failed")
	})
	if err != nil {
		t.Fatalf("HandleFetchRequestV4() error = %v", err)
	}
	wantBody, err := encodeFetchResponseV4(fetchResponseV4{Topics: []fetchTopicResponseV4{{Name: "orders", Partitions: []fetchPartitionResponseV4{{Index: 1, ErrorCode: -1, HighWatermark: -1, LastStableOffset: -1}}}}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("read-error Fetch response = %x, want %x", response, want)
	}
}

func TestHandleFetchRequestV4ReportsOffsetsBeyondHighWatermark(t *testing.T) {
	requestFor := func(offset int64) []byte {
		body := fetchRequestV4BodyForTest(fetchRequestV4{Topics: []fetchTopicRequestV4{{Name: "orders", Partitions: []fetchPartitionRequestV4{{Index: 0, FetchOffset: offset}}}}})
		return fetchRequestFrameV4ForTest(body)
	}
	tests := []struct {
		name     string
		offset   int64
		wantCode int16
		wantData []byte
	}{
		{name: "at high watermark remains valid", offset: 8, wantCode: 0, wantData: []byte{}},
		{name: "beyond high watermark is out of range", offset: 9, wantCode: 1, wantData: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := HandleFetchRequestV4(requestFor(test.offset), 1024, func(string, int, int64, int32) ([]byte, int64, error) {
				return []byte{}, 8, nil
			})
			if err != nil {
				t.Fatalf("HandleFetchRequestV4() error = %v", err)
			}
			wantBody, err := encodeFetchResponseV4(fetchResponseV4{Topics: []fetchTopicResponseV4{{Name: "orders", Partitions: []fetchPartitionResponseV4{{
				Index: 0, ErrorCode: test.wantCode, HighWatermark: 8, LastStableOffset: 8, Records: test.wantData,
			}}}}})
			if err != nil {
				t.Fatal(err)
			}
			want, err := encodeResponseFrame(append(encodeResponseHeaderV0(42), wantBody...), 1024)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(response, want) {
				t.Errorf("Fetch response = %x, want %x", response, want)
			}
		})
	}
}

func TestHandleFetchRequestV4RejectsInvalidRequests(t *testing.T) {
	valid := fetchRequestFrameV4ForTest(fetchRequestV4BodyForTest(fetchRequestV4{}))
	wrongAPI := append([]byte(nil), valid...)
	wrongAPI[1] = 0
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[3] = 3
	malformedBody := append([]byte(nil), valid[:10]...)
	for _, test := range []struct {
		name  string
		frame []byte
	}{
		{name: "invalid header", frame: []byte{0}},
		{name: "wrong API key", frame: wrongAPI},
		{name: "wrong API version", frame: wrongVersion},
		{name: "malformed body", frame: malformedBody},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleFetchRequestV4(test.frame, 1024, func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }); err == nil {
				t.Error("HandleFetchRequestV4() error = nil, want request error")
			}
		})
	}
	if _, err := HandleFetchRequestV4(valid, 1024, nil); err == nil {
		t.Error("HandleFetchRequestV4(nil reader) error = nil, want error")
	}
}

func TestHandleFetchRequestV4ReturnsResponseEncodingErrors(t *testing.T) {
	valid := fetchRequestFrameV4ForTest(fetchRequestV4BodyForTest(fetchRequestV4{}))
	t.Run("body encoder error", func(t *testing.T) {
		original := encodeFetchHandlerResponseBody
		encodeFetchHandlerResponseBody = func(fetchResponseV4) ([]byte, error) { return nil, errors.New("encode failed") }
		defer func() { encodeFetchHandlerResponseBody = original }()
		if _, err := HandleFetchRequestV4(valid, 1024, func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }); err == nil {
			t.Error("HandleFetchRequestV4() error = nil, want body encoding error")
		}
	})
	t.Run("outer frame size error", func(t *testing.T) {
		if _, err := HandleFetchRequestV4(valid, 1, func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }); err == nil {
			t.Error("HandleFetchRequestV4() error = nil, want frame-size error")
		}
	})
}

func fetchRequestFrameV4ForTest(body []byte) []byte {
	frame := make([]byte, 10, 10+len(body))
	binary.BigEndian.PutUint16(frame[0:2], 1)
	binary.BigEndian.PutUint16(frame[2:4], 4)
	binary.BigEndian.PutUint32(frame[4:8], 42)
	binary.BigEndian.PutUint16(frame[8:10], 0xffff)
	return append(frame, body...)
}
