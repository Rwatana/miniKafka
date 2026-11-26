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

func TestHandleKafkaRequestDispatchesMetadataV1(t *testing.T) {
	request := metadataRequestFrameV0(3, 1, 23, []byte{0xff, 0xff, 0xff, 0xff})
	response, err := HandleKafkaRequest(request, 1024, nil, nil, nil)
	if err != nil {
		t.Fatalf("HandleKafkaRequest(Metadata v1) error = %v", err)
	}
	want := []byte{0, 0, 0, 16, 0, 0, 0, 23, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}
	if !bytes.Equal(response, want) {
		t.Errorf("Metadata v1 response = %x, want %x", response, want)
	}

	for _, frame := range [][]byte{{0, 3}, metadataRequestFrameV0(3, 2, 23, []byte{0, 0, 0, 0})} {
		if _, err := HandleKafkaRequest(frame, 1024, nil, nil, nil); err == nil {
			t.Errorf("HandleKafkaRequest(Metadata %x) error = nil, want error", frame)
		}
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

func TestHandleKafkaRequestDispatchesAPIVersionsV0AndRejectsUnsupportedVersions(t *testing.T) {
	request := []byte{0, 18, 0, 0, 0, 0, 0, 9, 0xff, 0xff}
	supported := []APIVersionRange{{APIKey: 3, MinVersion: 0, MaxVersion: 0}}
	response, err := HandleKafkaRequest(request, 1024, supported, nil, nil)
	if err != nil {
		t.Fatalf("HandleKafkaRequest(ApiVersions v0) error = %v", err)
	}
	want := []byte{0, 0, 0, 16, 0, 0, 0, 9, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0, 0}
	if !bytes.Equal(response, want) {
		t.Errorf("ApiVersions v0 response = %x, want %x", response, want)
	}

	for _, frame := range [][]byte{{0, 18}, {0, 18, 0, 2}} {
		if _, err := HandleKafkaRequest(frame, 1024, supported, nil, nil); err == nil {
			t.Errorf("HandleKafkaRequest(ApiVersions %x) error = nil, want error", frame)
		}
	}
}

func TestHandleKafkaRequestDispatchesProduceV3(t *testing.T) {
	batch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	request := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: batch}}}})
	appended := 0
	response, err := HandleKafkaRequest(request, 1024, nil, nil, nil, func(topic string, partition int, got []byte) (int64, error) {
		appended++
		if topic != "orders" || partition != 0 || string(got) != string(batch) {
			t.Errorf("appender args = (%q, %d, %x), want (orders, 0, %x)", topic, partition, got, batch)
		}
		return 9, nil
	})
	if err != nil {
		t.Fatalf("HandleKafkaRequest() error = %v", err)
	}
	if appended != 1 {
		t.Errorf("appender calls = %d, want 1", appended)
	}
	want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{{Index: 0, BaseOffset: 9, LogAppendTimeMs: -1}}}}}, 1024)
	if err != nil {
		t.Fatalf("encode expected response: %v", err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("Produce response = %x, want %x", response, want)
	}
}

func TestHandleKafkaRequestRequiresProduceAppender(t *testing.T) {
	request := produceRequestFrameV3ForTest(nil, 1, nil)
	if _, err := HandleKafkaRequest(request, 1024, nil, nil, nil); err == nil {
		t.Error("HandleKafkaRequest(Produce without appender) error = nil, want explicit error")
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

func TestHandleKafkaRequestWithFetchReaderRoutesFetchAndPreservesExistingAPIs(t *testing.T) {
	fetchFrame := fetchRequestFrameV4ForTest(fetchRequestV4BodyForTest(fetchRequestV4{}))
	fetchResponse, err := HandleKafkaRequestWithFetchReader(fetchFrame, 1024, nil, nil, nil, nil, func(string, int, int64, int32) ([]byte, int64, error) {
		t.Error("reader called for empty Fetch request")
		return nil, 0, nil
	})
	if err != nil {
		t.Fatalf("HandleKafkaRequestWithFetchReader(Fetch) error = %v", err)
	}
	wantFetch, err := HandleFetchRequestV4(fetchFrame, 1024, func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fetchResponse, wantFetch) {
		t.Errorf("Fetch response = %x, want %x", fetchResponse, wantFetch)
	}

	apiVersions := []byte{0, 18, 0, 3, 0, 0, 0, 8, 1, 0, 2, 'n', 2, '1', 0}
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	delegated, err := HandleKafkaRequestWithFetchReader(apiVersions, 1024, supported, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("HandleKafkaRequestWithFetchReader(ApiVersions) error = %v", err)
	}
	wantDelegated, err := HandleKafkaRequest(apiVersions, 1024, supported, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(delegated, wantDelegated) {
		t.Errorf("delegated response = %x, want %x", delegated, wantDelegated)
	}
	produceBatch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	produce := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: produceBatch}}}})
	appended := false
	if _, err := HandleKafkaRequestWithFetchReader(produce, 1024, nil, nil, nil, func(string, int, []byte) (int64, error) {
		appended = true
		return 0, nil
	}, nil); err != nil {
		t.Fatalf("HandleKafkaRequestWithFetchReader(Produce) error = %v", err)
	}
	if !appended {
		t.Error("Produce appender was not called")
	}

	if _, err := HandleKafkaRequestWithFetchReader([]byte{0}, 1024, nil, nil, nil, nil, nil); err == nil {
		t.Error("short request error = nil, want dispatch error")
	}
	if _, err := HandleKafkaRequestWithFetchReader(fetchRequestFrameVersionForTest(fetchFrame, 3), 1024, nil, nil, nil, nil, func(string, int, int64, int32) ([]byte, int64, error) { return nil, 0, nil }); err == nil {
		t.Error("unsupported Fetch version error = nil, want handler error")
	}
	if _, err := HandleKafkaRequestWithFetchReader(fetchFrame, 1024, nil, nil, nil, nil, nil); err == nil {
		t.Error("missing Fetch reader error = nil, want handler error")
	}
}

func fetchRequestFrameVersionForTest(frame []byte, version byte) []byte {
	versioned := append([]byte(nil), frame...)
	versioned[3] = version
	return versioned
}
