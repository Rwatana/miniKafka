package kafka

import "fmt"

// decodeUncompressedRecordBatchV2 decodes the declared records in an uncompressed Kafka batch.
func decodeUncompressedRecordBatchV2(batch recordBatchV2) ([]kafkaRecordV2, error) {
	if batch.Attributes&0x07 != 0 {
		return nil, fmt.Errorf("decode Kafka record batch v2: compression type %d is unsupported", batch.Attributes&0x07)
	}
	if batch.RecordsCount < 0 {
		return nil, fmt.Errorf("decode Kafka record batch v2: negative record count %d", batch.RecordsCount)
	}
	if batch.RecordsCount > int32(len(batch.Records)/7) {
		return nil, fmt.Errorf("decode Kafka record batch v2: record count %d exceeds minimum record capacity", batch.RecordsCount)
	}
	records := make([]kafkaRecordV2, 0, batch.RecordsCount)
	consumed := 0
	previousOffset := int32(-1)
	for range int(batch.RecordsCount) {
		record, size, err := decodeRecordV2(batch.Records[consumed:])
		if err != nil {
			return nil, fmt.Errorf("decode Kafka record batch v2 record %d: %w", len(records), err)
		}
		if record.OffsetDelta < 0 {
			return nil, fmt.Errorf("decode Kafka record batch v2: negative offset delta %d", record.OffsetDelta)
		}
		if record.OffsetDelta <= previousOffset {
			return nil, fmt.Errorf("decode Kafka record batch v2: offset delta %d is not after %d", record.OffsetDelta, previousOffset)
		}
		if record.OffsetDelta > batch.LastOffsetDelta {
			return nil, fmt.Errorf("decode Kafka record batch v2: offset delta %d exceeds last offset delta %d", record.OffsetDelta, batch.LastOffsetDelta)
		}
		records = append(records, record)
		consumed += size
		previousOffset = record.OffsetDelta
	}
	if consumed != len(batch.Records) {
		return nil, fmt.Errorf("decode Kafka record batch v2: %d trailing record bytes", len(batch.Records)-consumed)
	}
	return records, nil
}
