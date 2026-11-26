package broker

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerLookupKafkaOffset(t *testing.T) {
	store := &Broker{DataDir: t.TempDir()}
	if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaTimestampBatchForTest(0)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		timestamp int64
		offset    int64
		recordTS  int64
	}{
		{name: "earliest", timestamp: -2, offset: 0, recordTS: 100},
		{name: "latest", timestamp: -1, offset: 2, recordTS: 150},
		{name: "first matching timestamp", timestamp: 100, offset: 0, recordTS: 100},
		{name: "second matching timestamp", timestamp: 101, offset: 1, recordTS: 150},
		{name: "no timestamp match", timestamp: 151, offset: -1, recordTS: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			offset, recordTimestamp, err := store.LookupKafkaOffset("orders", 0, test.timestamp)
			if err != nil || offset != test.offset || recordTimestamp != test.recordTS {
				t.Errorf("LookupKafkaOffset() = (%d, %d, %v), want (%d, %d, nil)", offset, recordTimestamp, err, test.offset, test.recordTS)
			}
		})
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerLookupKafkaOffsetUsesLogAppendTimestamp(t *testing.T) {
	store := &Broker{DataDir: t.TempDir()}
	if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaTimestampBatchForTest(8)); err != nil {
		t.Fatal(err)
	}
	offset, recordTimestamp, err := store.LookupKafkaOffset("orders", 0, 140)
	if err != nil || offset != 0 || recordTimestamp != 150 {
		t.Errorf("LookupKafkaOffset(log append time) = (%d, %d, %v), want (0, 150, nil)", offset, recordTimestamp, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerLookupKafkaOffsetHandlesEmptyAndMissingPartitions(t *testing.T) {
	directory := t.TempDir()
	emptyPath := filepath.Join(directory, "empty__0.kafka.log")
	emptyLog, err := storage.OpenKafkaPartitionLog(emptyPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CloseKafkaPartitionLog(emptyLog); err != nil {
		t.Fatal(err)
	}
	store := &Broker{DataDir: directory}
	for _, timestamp := range []int64{-2, -1} {
		offset, recordTimestamp, err := store.LookupKafkaOffset("empty", 0, timestamp)
		if err != nil || offset != 0 || recordTimestamp != -1 {
			t.Errorf("LookupKafkaOffset(empty, %d) = (%d, %d, %v), want (0, -1, nil)", timestamp, offset, recordTimestamp, err)
		}
	}
	if offset, recordTimestamp, err := store.LookupKafkaOffset("empty", 0, 123); err != nil || offset != -1 || recordTimestamp != -1 {
		t.Errorf("LookupKafkaOffset(empty, timestamp) = (%d, %d, %v), want (-1, -1, nil)", offset, recordTimestamp, err)
	}
	if _, _, err := store.LookupKafkaOffset("missing", 0, -2); !errors.Is(err, kafkaerr.ErrUnknownTopicOrPartition) {
		t.Errorf("LookupKafkaOffset(missing) error = %v, want unknown topic/partition", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerLookupKafkaOffsetRejectsInvalidQueriesAndCompressedBatches(t *testing.T) {
	if _, _, err := (*Broker)(nil).LookupKafkaOffset("orders", 0, -2); err == nil {
		t.Error("LookupKafkaOffset(nil) error = nil, want error")
	}
	if _, _, err := (&Broker{DataDir: t.TempDir()}).LookupKafkaOffset("orders", 0, -3); err == nil {
		t.Error("LookupKafkaOffset(timestamp=-3) error = nil, want error")
	}
	directory := t.TempDir()
	store := &Broker{DataDir: directory}
	if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaTimestampBatchForTest(1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LookupKafkaOffset("orders", 0, 0); err == nil {
		t.Error("LookupKafkaOffset(compressed timestamp query) error = nil, want explicit unsupported error")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerLookupKafkaOffsetRejectsMalformedStoredRecord(t *testing.T) {
	directory := t.TempDir()
	store := &Broker{DataDir: directory}
	if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaTimestampBatchForTest(0)); err != nil {
		t.Fatal(err)
	}
	originalRead := readBrokerKafkaRecordBatches
	defer func() { readBrokerKafkaRecordBatches = originalRead }()
	readBrokerKafkaRecordBatches = func(_ *storage.KafkaPartitionLog, _ int64, _ int32) ([]byte, int64, error) {
		return []byte{0, 0, 0, 0}, 0, nil
	}
	if _, _, err := store.LookupKafkaOffset("orders", 0, -1); err == nil {
		t.Error("LookupKafkaOffset(malformed stored batch) error = nil, want validation error")
	}
	readBrokerKafkaRecordBatches = originalRead
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerLookupKafkaOffsetRejectsMalformedBatchAndRecordFields(t *testing.T) {
	var oversizedLength [binary.MaxVarintLen64]byte
	oversizedLengthBytes := binary.PutUvarint(oversizedLength[:], uint64(1)<<32)
	tests := []struct {
		name      string
		batch     []byte
		timestamp int64
	}{
		{name: "trailing partial batch header", batch: append(fakeLookupBatch(0, 0, 0, 0, 0, 0, nil), 1, 2, 3, 4), timestamp: -1},
		{name: "invalid batch length", batch: fakeLookupBatchWithLength(0, 0, 0, 0, 0, 0, nil, 48), timestamp: -1},
		{name: "batch length exceeds bytes", batch: fakeLookupBatchWithLength(0, 0, 0, 0, 0, 0, nil, 50), timestamp: -1},
		{name: "unsupported magic", batch: func() []byte { b := fakeLookupBatch(0, 0, 0, 0, 0, 0, nil); b[16] = 1; return b }(), timestamp: -1},
		{name: "truncated record length", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{0x80}), timestamp: 0},
		{name: "record length varint overflow", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, oversizedLength[:oversizedLengthBytes]), timestamp: 0},
		{name: "negative record length", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{1}), timestamp: 0},
		{name: "record size exceeds bytes", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{200}), timestamp: 0},
		{name: "missing record attributes", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{0}), timestamp: 0},
		{name: "invalid timestamp varlong", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{2, 0, 0x80}), timestamp: 0},
		{name: "invalid offset varint", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{4, 0, 0}), timestamp: 0},
		{name: "offset varint overflow", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{14, 0, 0, 0x80, 0x80, 0x80, 0x80, 0x10}), timestamp: 0},
		{name: "timestamp overflow", batch: fakeLookupBatch(0, int64(^uint64(0)>>1), int64(^uint64(0)>>1), 0, 0, 1, []byte{6, 0, 2, 0}), timestamp: 0},
		{name: "negative offset delta", batch: fakeLookupBatch(0, 0, 0, 0, 0, 1, []byte{6, 0, 0, 1}), timestamp: 0},
		{name: "offset overflow", batch: fakeLookupBatch(int64(^uint64(0)>>1), 0, 0, 0, 0, 1, []byte{6, 0, 0, 2}), timestamp: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runFakeKafkaOffsetLookup(t, test.batch, test.timestamp); err == nil {
				t.Error("LookupKafkaOffset() error = nil, want malformed batch error")
			}
		})
	}
}

func runFakeKafkaOffsetLookup(t *testing.T, batches []byte, timestamp int64) error {
	t.Helper()
	store := &Broker{DataDir: t.TempDir()}
	if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaTimestampBatchForTest(0)); err != nil {
		t.Fatal(err)
	}
	originalRead := readBrokerKafkaRecordBatches
	readBrokerKafkaRecordBatches = func(_ *storage.KafkaPartitionLog, _ int64, _ int32) ([]byte, int64, error) {
		return batches, 0, nil
	}
	_, _, err := store.LookupKafkaOffset("orders", 0, timestamp)
	readBrokerKafkaRecordBatches = originalRead
	if closeErr := store.Close(); closeErr != nil {
		t.Errorf("close broker: %v", closeErr)
	}
	return err
}

func fakeLookupBatch(baseOffset, baseTimestamp, maxTimestamp int64, attributes, lastOffsetDelta, recordCount int32, records []byte) []byte {
	return fakeLookupBatchWithLength(baseOffset, baseTimestamp, maxTimestamp, attributes, lastOffsetDelta, recordCount, records, int32(49+len(records)))
}

func fakeLookupBatchWithLength(baseOffset, baseTimestamp, maxTimestamp int64, attributes, lastOffsetDelta, recordCount int32, records []byte, batchLength int32) []byte {
	batch := make([]byte, 61+len(records))
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], uint32(batchLength))
	batch[16] = 2
	binary.BigEndian.PutUint16(batch[21:23], uint16(attributes))
	binary.BigEndian.PutUint32(batch[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint64(batch[27:35], uint64(baseTimestamp))
	binary.BigEndian.PutUint64(batch[35:43], uint64(maxTimestamp))
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))
	copy(batch[61:], records)
	return batch
}

func kafkaTimestampBatchForTest(codec int16) []byte {
	encodeSignedVarint := func(value int32) []byte {
		var encoded [binary.MaxVarintLen32]byte
		uval := uint32(value<<1) ^ uint32(value>>31)
		n := binary.PutUvarint(encoded[:], uint64(uval))
		return encoded[:n]
	}
	encodeSignedVarlong := func(value int64) []byte {
		var encoded [binary.MaxVarintLen64]byte
		uval := uint64(value<<1) ^ uint64(value>>63)
		n := binary.PutUvarint(encoded[:], uval)
		return encoded[:n]
	}
	encodeRecord := func(timestampDelta int64, offsetDelta int32, value byte) []byte {
		record := []byte{0}
		record = append(record, encodeSignedVarlong(timestampDelta)...)
		record = append(record, encodeSignedVarint(offsetDelta)...)
		record = append(record, encodeSignedVarint(-1)...)
		record = append(record, encodeSignedVarint(1)...)
		record = append(record, value)
		record = append(record, encodeSignedVarint(0)...)
		encoded := encodeSignedVarint(int32(len(record)))
		return append(encoded, record...)
	}
	records := append(encodeRecord(0, 0, 'a'), encodeRecord(50, 1, 'b')...)
	batch := make([]byte, 61+len(records))
	binary.BigEndian.PutUint32(batch[8:12], uint32(49+len(records)))
	batch[16] = 2
	binary.BigEndian.PutUint16(batch[21:23], uint16(codec))
	binary.BigEndian.PutUint32(batch[23:27], 1)
	binary.BigEndian.PutUint64(batch[27:35], 100)
	binary.BigEndian.PutUint64(batch[35:43], 150)
	binary.BigEndian.PutUint32(batch[57:61], 2)
	copy(batch[61:], records)
	binary.BigEndian.PutUint32(batch[17:21], crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)))
	return batch
}
