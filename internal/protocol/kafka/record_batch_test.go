package kafka

import (
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

func TestDecodeRecordBatchV2(t *testing.T) {
	batch := recordBatchV2ForTest(1, 1, []byte{0xaa, 0xbb})
	got, err := decodeRecordBatchV2(batch)
	if err != nil {
		t.Fatalf("decodeRecordBatchV2() error = %v", err)
	}
	want := recordBatchV2{
		BaseOffset:           5,
		BatchLength:          51,
		PartitionLeaderEpoch: 2,
		CRC:                  binary.BigEndian.Uint32(batch[17:21]),
		Attributes:           1,
		LastOffsetDelta:      0,
		BaseTimestamp:        100,
		MaxTimestamp:         150,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		RecordsCount:         1,
		Records:              []byte{0xaa, 0xbb},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decodeRecordBatchV2() = %+v, want %+v", got, want)
	}

	empty, err := decodeRecordBatchV2(recordBatchV2ForTest(0, 0, nil))
	if err != nil {
		t.Fatalf("decodeRecordBatchV2(empty) error = %v", err)
	}
	if empty.RecordsCount != 0 || len(empty.Records) != 0 {
		t.Errorf("empty batch = %+v, want zero records", empty)
	}
}

func TestDecodeRecordBatchV2RejectsMalformedBatch(t *testing.T) {
	valid := recordBatchV2ForTest(1, 0, []byte{1})
	short := valid[:60]
	tooSmall := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(tooSmall[8:12], 48)
	negativeLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(negativeLength[8:12], ^uint32(0))
	wrongLength := append(append([]byte(nil), valid...), 0)
	wrongMagic := append([]byte(nil), valid...)
	wrongMagic[16] = 1
	badCRC := append([]byte(nil), valid...)
	badCRC[21] ^= 0x01
	negativeCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(negativeCount[57:61], ^uint32(0))
	updateRecordBatchCRCForTest(negativeCount)

	tests := []struct {
		name  string
		batch []byte
	}{
		{name: "truncated fixed header", batch: short},
		{name: "batch length below minimum", batch: tooSmall},
		{name: "negative batch length", batch: negativeLength},
		{name: "batch length does not match bytes", batch: wrongLength},
		{name: "wrong magic", batch: wrongMagic},
		{name: "invalid CRC32C", batch: badCRC},
		{name: "negative record count", batch: negativeCount},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRecordBatchV2(test.batch); err == nil {
				t.Error("decodeRecordBatchV2() error = nil, want malformed-batch error")
			}
		})
	}
}

func recordBatchV2ForTest(recordsCount int32, attributes int16, records []byte) []byte {
	batch := make([]byte, 61+len(records))
	binary.BigEndian.PutUint64(batch[0:8], 5)
	binary.BigEndian.PutUint32(batch[8:12], uint32(49+len(records)))
	binary.BigEndian.PutUint32(batch[12:16], 2)
	batch[16] = 2
	binary.BigEndian.PutUint16(batch[21:23], uint16(attributes))
	binary.BigEndian.PutUint32(batch[23:27], 0)
	binary.BigEndian.PutUint64(batch[27:35], 100)
	binary.BigEndian.PutUint64(batch[35:43], 150)
	binary.BigEndian.PutUint64(batch[43:51], ^uint64(0))
	binary.BigEndian.PutUint16(batch[51:53], ^uint16(0))
	binary.BigEndian.PutUint32(batch[53:57], ^uint32(0))
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordsCount))
	copy(batch[61:], records)
	updateRecordBatchCRCForTest(batch)
	return batch
}

func updateRecordBatchCRCForTest(batch []byte) {
	checksum := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], checksum)
}
