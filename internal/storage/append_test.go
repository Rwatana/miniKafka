package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestAppendRecord(t *testing.T) {
	t.Run("assigns offsets and persists records", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "topic__0.log")
		log, err := OpenPartitionLog(path)
		if err != nil {
			t.Fatal(err)
		}
		key := "customer-42"
		startedAt := time.Now().UnixMilli()
		firstOffset, err := AppendRecord(log, &key, "order-created")
		if err != nil {
			t.Fatalf("first AppendRecord() error = %v", err)
		}
		if firstOffset != 0 {
			t.Errorf("first offset = %d, want 0", firstOffset)
		}
		secondOffset, err := AppendRecord(log, nil, "anonymous-event")
		if err != nil {
			t.Fatalf("second AppendRecord() error = %v", err)
		}
		if secondOffset != 1 {
			t.Errorf("second offset = %d, want 1", secondOffset)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(contents, []byte("\n"))
		if len(lines) != 3 || len(lines[2]) != 0 {
			t.Fatalf("JSONL contents = %q, want two newline-terminated records", contents)
		}
		firstRecord, err := decodeRecord(lines[0])
		if err != nil {
			t.Fatal(err)
		}
		secondRecord, err := decodeRecord(lines[1])
		if err != nil {
			t.Fatal(err)
		}
		if firstRecord.Key == nil || *firstRecord.Key != key || firstRecord.Value != "order-created" {
			t.Errorf("first record = %+v, want keyed order-created record", firstRecord)
		}
		if secondRecord.Key != nil || secondRecord.Value != "anonymous-event" {
			t.Errorf("second record = %+v, want null-key anonymous-event record", secondRecord)
		}
		finishedAt := time.Now().UnixMilli()
		if firstRecord.Timestamp < startedAt || firstRecord.Timestamp > finishedAt || secondRecord.Timestamp < startedAt || secondRecord.Timestamp > finishedAt {
			t.Errorf("timestamps = %d, %d; want current Unix millisecond timestamps in [%d, %d]", firstRecord.Timestamp, secondRecord.Timestamp, startedAt, finishedAt)
		}
		wantPositions := []int64{0, int64(len(lines[0]) + 1)}
		if !reflect.DeepEqual(log.positions, wantPositions) {
			t.Errorf("positions = %v, want %v", log.positions, wantPositions)
		}
		if err := ClosePartitionLog(log); err != nil {
			t.Fatal(err)
		}

		reopened, err := OpenPartitionLog(path)
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(reopened)
		if !reflect.DeepEqual(reopened.positions, wantPositions) {
			t.Errorf("recovered positions = %v, want %v", reopened.positions, wantPositions)
		}
	})

	t.Run("nil log", func(t *testing.T) {
		if offset, err := AppendRecord(nil, nil, "value"); offset != 0 || err == nil {
			t.Errorf("AppendRecord(nil) = (%d, %v), want error", offset, err)
		}
	})

	t.Run("nil file", func(t *testing.T) {
		if offset, err := AppendRecord(&PartitionLog{}, nil, "value"); offset != 0 || err == nil {
			t.Errorf("AppendRecord(empty log) = (%d, %v), want error", offset, err)
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
		if _, err := AppendRecord(log, nil, "value"); !errors.Is(err, os.ErrClosed) {
			t.Errorf("AppendRecord() error = %v, want closed-file error", err)
		}
		if len(log.positions) != 0 {
			t.Errorf("positions = %v, want empty index after failure", log.positions)
		}
	})

	t.Run("encoding failure", func(t *testing.T) {
		log, err := OpenPartitionLog(filepath.Join(t.TempDir(), "encode.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer ClosePartitionLog(log)
		originalEncoder := partitionRecordEncoder
		partitionRecordEncoder = func(Record) ([]byte, error) { return nil, errors.New("encode failed") }
		defer func() { partitionRecordEncoder = originalEncoder }()

		if _, err := AppendRecord(log, nil, "value"); err == nil {
			t.Fatal("AppendRecord() error = nil, want encoding error")
		}
		if len(log.positions) != 0 {
			t.Errorf("positions = %v, want empty index after failure", log.positions)
		}
	})

	t.Run("write failure", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "readonly.log")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		log := &PartitionLog{file: file}
		defer file.Close()
		if _, err := AppendRecord(log, nil, "value"); err == nil {
			t.Fatal("AppendRecord() error = nil, want write error")
		}
		if len(log.positions) != 0 {
			t.Errorf("positions = %v, want empty index after failure", log.positions)
		}
	})
}
