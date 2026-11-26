package kafka

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestHandleProduceRequestV3AppendsValidatedBatches(t *testing.T) {
	first := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	second := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{OffsetDelta: 0, Key: []byte("k"), Value: []byte("v"), Headers: []recordHeaderV2{}}))
	request := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: append(first, second...)}, {Index: 1, Records: first}}}})
	var appended [][]byte
	var partitions []int
	nextOffset := int64(0)
	response, err := HandleProduceRequestV3(request, 1024, func(topic string, partition int, batch []byte) (int64, error) {
		if topic != "orders" {
			t.Errorf("appended topic = %q, want orders", topic)
		}
		partitions = append(partitions, partition)
		appended = append(appended, bytes.Clone(batch))
		base := nextOffset
		nextOffset++
		return base, nil
	})
	if err != nil {
		t.Fatalf("HandleProduceRequestV3() error = %v", err)
	}
	if !reflect.DeepEqual(partitions, []int{0, 0, 1}) {
		t.Errorf("appended partitions = %v, want [0 0 1]", partitions)
	}
	if !reflect.DeepEqual(appended, [][]byte{first, second, first}) {
		t.Errorf("appended batch bytes = %x, want original batch order", appended)
	}
	want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{
		{Index: 0, BaseOffset: 0, LogAppendTimeMs: -1},
		{Index: 1, BaseOffset: 2, LogAppendTimeMs: -1},
	}}}}, 1024)
	if err != nil {
		t.Fatalf("encode expected response: %v", err)
	}
	if !bytes.Equal(response, want) {
		t.Errorf("Produce response = %x, want %x", response, want)
	}
}

func TestHandleProduceRequestV3ReturnsNoResponseForAcksZero(t *testing.T) {
	batch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	request := produceRequestFrameV3ForTest(nil, 0, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: batch}}}})
	appended := 0
	response, err := HandleProduceRequestV3(request, 1024, func(string, int, []byte) (int64, error) {
		appended++
		return 0, nil
	})
	if err != nil || response != nil || appended != 1 {
		t.Errorf("HandleProduceRequestV3(acks=0) = (%x, %v), appends %d; want nil response, success, one append", response, err, appended)
	}
}

func TestHandleProduceRequestV3RejectsInvalidRecordsAndReportsAppendErrors(t *testing.T) {
	validBatch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	invalidBatch := append([]byte(nil), validBatch...)
	invalidBatch[22] = 1
	updateRecordBatchCRCForTest(invalidBatch)
	t.Run("invalid record set is not appended", func(t *testing.T) {
		request := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: invalidBatch}, {Index: 1, Records: nil}}}})
		appended := 0
		response, err := HandleProduceRequestV3(request, 1024, func(string, int, []byte) (int64, error) {
			appended++
			return 0, nil
		})
		if err != nil {
			t.Fatalf("HandleProduceRequestV3() error = %v", err)
		}
		if appended != 0 {
			t.Errorf("appender calls = %d, want 0", appended)
		}
		want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{
			{Index: 0, ErrorCode: 87, BaseOffset: -1, LogAppendTimeMs: -1},
			{Index: 1, ErrorCode: 87, BaseOffset: -1, LogAppendTimeMs: -1},
		}}}}, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(response, want) {
			t.Errorf("invalid-record response = %x, want %x", response, want)
		}
	})

	t.Run("append failure maps to unknown server error", func(t *testing.T) {
		request := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: validBatch}}}})
		response, err := HandleProduceRequestV3(request, 1024, func(string, int, []byte) (int64, error) { return 0, errors.New("disk failure") })
		if err != nil {
			t.Fatalf("HandleProduceRequestV3() error = %v", err)
		}
		want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{{Index: 0, ErrorCode: -1, BaseOffset: -1, LogAppendTimeMs: -1}}}}}, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(response, want) {
			t.Errorf("append-error response = %x, want %x", response, want)
		}
	})
}

func TestHandleProduceRequestV3RejectsInvalidRequests(t *testing.T) {
	valid := produceRequestFrameV3ForTest(nil, 1, nil)
	wrongKey := append([]byte(nil), valid...)
	wrongKey[1] = 3
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[3] = 2
	malformedBody := append([]byte(nil), valid[:10]...)
	transactionalID := "tx"
	transactional := produceRequestFrameV3ForTest(&transactionalID, 1, nil)
	for _, test := range []struct {
		name  string
		frame []byte
	}{
		{name: "invalid header", frame: []byte{0}},
		{name: "wrong API key", frame: wrongKey},
		{name: "wrong API version", frame: wrongVersion},
		{name: "malformed body", frame: malformedBody},
		{name: "transaction ID", frame: transactional},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := HandleProduceRequestV3(test.frame, 1024, func(string, int, []byte) (int64, error) { return 0, nil }); err == nil {
				t.Error("HandleProduceRequestV3() error = nil, want request error")
			}
		})
	}
	if _, err := HandleProduceRequestV3(valid, 1024, nil); err == nil {
		t.Error("HandleProduceRequestV3(nil appender) error = nil, want error")
	}
	request := produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{}}})
	if _, err := HandleProduceRequestV3(request, 1, func(string, int, []byte) (int64, error) { return 0, nil }); err == nil {
		t.Error("HandleProduceRequestV3(oversized) error = nil, want frame-size error")
	}
}

func produceRequestFrameV3ForTest(transactionalID *string, acks int16, topics []produceTopicData) []byte {
	body := produceRequestV3BodyForTest(transactionalID, acks, 1000, topics)
	frame := make([]byte, 10, 10+len(body))
	frame[1] = 0
	frame[3] = 3
	frame[7] = 42
	frame[8] = 0xff
	frame[9] = 0xff
	return append(frame, body...)
}
