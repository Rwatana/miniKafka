package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestAppendRecords(t *testing.T) {
	t.Run("writes a batch with contiguous offsets", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "batch.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)

		key := "batch-key"
		startedAt := time.Now().UnixMilli()
		originalWrite := partitionLogWrite
		writeCount := 0
		partitionLogWrite = func(file *os.File, data []byte) (int, error) {
			writeCount++
			return originalWrite(file, data)
		}
		defer func() { partitionLogWrite = originalWrite }()
		offsets, err := AppendRecords(log, []Record{{Key: &key, Value: "first"}, {Value: "second"}})
		if err != nil {
			t.Fatalf("AppendRecords() error = %v", err)
		}
		if !reflect.DeepEqual(offsets, []int64{0, 1}) {
			t.Fatalf("offsets = %v, want [0 1]", offsets)
		}
		if writeCount != 1 {
			t.Errorf("partition writes = %d, want one batched write", writeCount)
		}
		records, err := ReadFromOffset(log, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 || records[0].Key == nil || *records[0].Key != key || records[0].Value != "first" || records[1].Key != nil || records[1].Value != "second" {
			t.Errorf("records = %+v, want keyed and null-key batch records", records)
		}
		finishedAt := time.Now().UnixMilli()
		for _, record := range records {
			if record.Timestamp < startedAt || record.Timestamp > finishedAt {
				t.Errorf("timestamp = %d, want current Unix milliseconds in [%d, %d]", record.Timestamp, startedAt, finishedAt)
			}
		}
		if len(log.positions) != 2 || log.positions[0] != 0 || log.positions[1] <= log.positions[0] {
			t.Errorf("positions = %v, want two increasing byte offsets", log.positions)
		}
	})

	t.Run("empty batch does not write", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "empty.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)
		offsets, err := AppendRecords(log, nil)
		if err != nil || len(offsets) != 0 {
			t.Errorf("AppendRecords(empty) = (%v, %v), want empty offsets and no error", offsets, err)
		}
		info, err := log.file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 || len(log.positions) != 0 {
			t.Errorf("empty batch changed log state: size %d, positions %v", info.Size(), log.positions)
		}
	})

	t.Run("nil log", func(t *testing.T) {
		if offsets, err := AppendRecords(nil, []Record{{Value: "value"}}); offsets != nil || err == nil {
			t.Errorf("AppendRecords(nil) = (%v, %v), want error", offsets, err)
		}
	})

	t.Run("seek failure", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "seek.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := log.file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendRecords(log, []Record{{Value: "value"}}); !errors.Is(err, os.ErrClosed) {
			t.Errorf("AppendRecords() error = %v, want closed-file error", err)
		}
	})

	t.Run("encoding failure leaves index unchanged", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "encode.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)
		originalEncoder := partitionRecordEncoder
		calls := 0
		partitionRecordEncoder = func(record Record) ([]byte, error) {
			calls++
			if calls == 2 {
				return nil, errors.New("encode failed")
			}
			return originalEncoder(record)
		}
		defer func() { partitionRecordEncoder = originalEncoder }()
		if _, err := AppendRecords(log, []Record{{Value: "first"}, {Value: "second"}}); err == nil {
			t.Fatal("AppendRecords() error = nil, want encoding error")
		}
		if len(log.positions) != 0 {
			t.Errorf("positions = %v, want empty index", log.positions)
		}
	})

	t.Run("short write rolls back batch", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "write.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)
		originalWrite := partitionLogWrite
		partitionLogWrite = func(file *os.File, data []byte) (int, error) {
			return originalWrite(file, data[:len(data)/2])
		}
		defer func() { partitionLogWrite = originalWrite }()
		if _, err := AppendRecords(log, []Record{{Value: "partial"}}); err == nil {
			t.Fatal("AppendRecords() error = nil, want short-write error")
		}
		info, err := log.file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 || len(log.positions) != 0 {
			t.Errorf("failed batch left size %d and positions %v, want zero", info.Size(), log.positions)
		}
	})
}
