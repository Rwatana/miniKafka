package kafka

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type recordBatchV2 struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsCount         int32
	Records              []byte
}

// decodeRecordBatchV2 validates and decodes one Kafka magic-v2 record batch header.
func decodeRecordBatchV2(batch []byte) (recordBatchV2, error) {
	const headerSize = 61
	const minimumBatchLength = headerSize - 12
	if len(batch) < headerSize {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: size %d is smaller than header size %d", len(batch), headerSize)
	}
	batchLength := int32(binary.BigEndian.Uint32(batch[8:12]))
	if batchLength < minimumBatchLength {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: batch length %d is smaller than minimum %d", batchLength, minimumBatchLength)
	}
	if int64(batchLength)+12 != int64(len(batch)) {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: batch length %d does not match actual size %d", batchLength, len(batch)-12)
	}
	if batch[16] != 2 {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: magic %d, want 2", batch[16])
	}
	checksum := binary.BigEndian.Uint32(batch[17:21])
	if actual := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)); actual != checksum {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: CRC32C mismatch: got %08x, want %08x", actual, checksum)
	}
	recordsCount := int32(binary.BigEndian.Uint32(batch[57:61]))
	if recordsCount < 0 {
		return recordBatchV2{}, fmt.Errorf("decode Kafka record batch v2: negative record count %d", recordsCount)
	}
	return recordBatchV2{
		BaseOffset:           int64(binary.BigEndian.Uint64(batch[0:8])),
		BatchLength:          batchLength,
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(batch[12:16])),
		CRC:                  checksum,
		Attributes:           int16(binary.BigEndian.Uint16(batch[21:23])),
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(batch[23:27])),
		BaseTimestamp:        int64(binary.BigEndian.Uint64(batch[27:35])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(batch[35:43])),
		ProducerID:           int64(binary.BigEndian.Uint64(batch[43:51])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(batch[51:53])),
		BaseSequence:         int32(binary.BigEndian.Uint32(batch[53:57])),
		RecordsCount:         recordsCount,
		Records:              batch[headerSize:],
	}, nil
}
