package kafka

import (
	"reflect"
	"testing"
)

func TestDecodeRecordBatchesV2(t *testing.T) {
	first := recordBatchV2ForTest(0, 0, nil)
	second := recordBatchV2ForTest(1, 0, []byte{0xaa, 0xbb})
	got, err := decodeRecordBatchesV2(append(first, second...))
	if err != nil {
		t.Fatalf("decodeRecordBatchesV2() error = %v", err)
	}
	if len(got) != 2 || got[0].RecordsCount != 0 || got[1].RecordsCount != 1 || !reflect.DeepEqual(got[1].Records, []byte{0xaa, 0xbb}) {
		t.Errorf("batches = %+v, want two ordered decoded batches", got)
	}
}

func TestDecodeRecordBatchesV2RejectsMalformedRecordSet(t *testing.T) {
	valid := recordBatchV2ForTest(0, 0, nil)
	truncatedPayload := append([]byte(nil), valid[:len(valid)-1]...)
	oversizedBatch := append([]byte(nil), valid...)
	oversizedBatch[11]++
	undersizedBatch := append([]byte(nil), valid...)
	undersizedBatch[11] = 48
	badCRC := append([]byte(nil), valid...)
	badCRC[21] ^= 1
	tests := []struct {
		name    string
		records []byte
	}{
		{name: "empty record set", records: nil},
		{name: "truncated batch prefix", records: valid[:11]},
		{name: "truncated batch payload", records: truncatedPayload},
		{name: "declared batch is too large", records: oversizedBatch},
		{name: "batch length below minimum", records: undersizedBatch},
		{name: "invalid batch crc", records: badCRC},
		{name: "trailing truncated prefix", records: append(append([]byte(nil), valid...), 1, 2, 3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRecordBatchesV2(test.records); err == nil {
				t.Error("decodeRecordBatchesV2() error = nil, want malformed-record-set error")
			}
		})
	}
}
