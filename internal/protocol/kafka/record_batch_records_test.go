package kafka

import (
	"reflect"
	"testing"
)

func TestDecodeUncompressedRecordBatchV2(t *testing.T) {
	first := kafkaRecordV2{OffsetDelta: 0, Key: []byte("a"), Value: []byte("one"), Headers: []recordHeaderV2{}}
	second := kafkaRecordV2{TimestampDelta: 3, OffsetDelta: 2, Key: nil, Value: []byte("two"), Headers: []recordHeaderV2{}}
	batch := recordBatchV2{
		LastOffsetDelta: 2,
		RecordsCount:    2,
		Records:         append(recordV2ForTest(first), recordV2ForTest(second)...),
	}
	got, err := decodeUncompressedRecordBatchV2(batch)
	if err != nil {
		t.Fatalf("decodeUncompressedRecordBatchV2() error = %v", err)
	}
	if want := []kafkaRecordV2{first, second}; !reflect.DeepEqual(got, want) {
		t.Errorf("records = %+v, want %+v", got, want)
	}

	empty, err := decodeUncompressedRecordBatchV2(recordBatchV2{})
	if err != nil {
		t.Fatalf("decodeUncompressedRecordBatchV2(empty) error = %v", err)
	}
	if !reflect.DeepEqual(empty, []kafkaRecordV2{}) {
		t.Errorf("empty records = %+v, want empty result", empty)
	}
}

func TestDecodeUncompressedRecordBatchV2RejectsInvalidBatch(t *testing.T) {
	validRecord := recordV2ForTest(kafkaRecordV2{OffsetDelta: 0, Headers: []recordHeaderV2{}})
	badRecord := append(wrapRecordDataForTest([]byte{0, 0x80}), 0, 0, 0, 0)
	firstOffsetOne := recordV2ForTest(kafkaRecordV2{OffsetDelta: 1, Headers: []recordHeaderV2{}})
	secondOffsetZero := recordV2ForTest(kafkaRecordV2{OffsetDelta: 0, Headers: []recordHeaderV2{}})
	tests := []struct {
		name  string
		batch recordBatchV2
	}{
		{name: "compressed batch", batch: recordBatchV2{Attributes: 1}},
		{name: "negative record count", batch: recordBatchV2{RecordsCount: -1}},
		{name: "record count exceeds payload capacity", batch: recordBatchV2{RecordsCount: 1, Records: []byte{0}}},
		{name: "malformed record", batch: recordBatchV2{RecordsCount: 1, Records: badRecord}},
		{name: "negative offset delta", batch: recordBatchV2{RecordsCount: 1, Records: recordV2ForTest(kafkaRecordV2{OffsetDelta: -1, Headers: []recordHeaderV2{}})}},
		{name: "offset delta exceeds last offset", batch: recordBatchV2{RecordsCount: 1, Records: validRecord, LastOffsetDelta: -1}},
		{name: "offset deltas are out of order", batch: recordBatchV2{RecordsCount: 2, LastOffsetDelta: 1, Records: append(firstOffsetOne, secondOffsetZero...)}},
		{name: "missing record bytes", batch: recordBatchV2{RecordsCount: 1}},
		{name: "trailing record bytes", batch: recordBatchV2{Records: []byte{0xff}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeUncompressedRecordBatchV2(test.batch); err == nil {
				t.Error("decodeUncompressedRecordBatchV2() error = nil, want error")
			}
		})
	}
}
