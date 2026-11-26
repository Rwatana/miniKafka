package kafka

import (
	"encoding/binary"
	"fmt"
)

// decodeRecordBatchesV2 decodes all length-delimited magic-v2 batches in a record set.
func decodeRecordBatchesV2(encoded []byte) ([]recordBatchV2, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("decode Kafka record batches v2: empty record set")
	}
	batches := make([]recordBatchV2, 0)
	for offset := 0; offset < len(encoded); {
		remaining := encoded[offset:]
		if len(remaining) < 12 {
			return nil, fmt.Errorf("decode Kafka record batches v2: truncated batch prefix at byte %d", offset)
		}
		batchLength := int32(binary.BigEndian.Uint32(remaining[8:12]))
		if batchLength < 49 {
			return nil, fmt.Errorf("decode Kafka record batches v2: batch length %d is smaller than minimum 49", batchLength)
		}
		encodedLength := int64(batchLength) + 12
		if encodedLength > int64(len(remaining)) {
			return nil, fmt.Errorf("decode Kafka record batches v2: batch at byte %d declares %d bytes, only %d remain", offset, encodedLength, len(remaining))
		}
		batch, err := decodeRecordBatchV2(remaining[:int(encodedLength)])
		if err != nil {
			return nil, fmt.Errorf("decode Kafka record batches v2 batch %d: %w", len(batches), err)
		}
		batches = append(batches, batch)
		offset += int(encodedLength)
	}
	return batches, nil
}
