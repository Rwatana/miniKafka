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
