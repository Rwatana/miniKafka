package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestReadFromOffset(t *testing.T) {
	t.Run("reads an ordered bounded range", func(t *testing.T) {
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one")+encodedTestRecord(t, 1, "two")+encodedTestRecord(t, 2, "three"))
		got, err := ReadFromOffset(log, 1, 1)
		if err != nil {
			t.Fatalf("ReadFromOffset() error = %v", err)
		}
		want := []Record{{Offset: 1, Value: "two", Timestamp: 2}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ReadFromOffset() = %+v, want %+v", got, want)
		}
	})

	t.Run("reads through the end when maximum exceeds remaining records", func(t *testing.T) {
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one")+strings.TrimSuffix(encodedTestRecord(t, 1, "two"), "\n"))
		got, err := ReadFromOffset(log, 1, 100)
		if err != nil {
			t.Fatalf("ReadFromOffset() error = %v", err)
		}
		if len(got) != 1 || got[0].Value != "two" {
			t.Errorf("ReadFromOffset() = %+v, want one record with value two", got)
		}
	})

	t.Run("returns empty at or beyond the end", func(t *testing.T) {
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one"))
		for _, offset := range []int64{1, 9} {
			got, err := ReadFromOffset(log, offset, 10)
			if err != nil {
				t.Errorf("ReadFromOffset(%d) error = %v", offset, err)
			}
			if len(got) != 0 {
				t.Errorf("ReadFromOffset(%d) = %v, want empty", offset, got)
			}
		}
	})

	t.Run("returns empty for zero maximum", func(t *testing.T) {
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one"))
		got, err := ReadFromOffset(log, 0, 0)
		if err != nil || len(got) != 0 {
			t.Errorf("ReadFromOffset() = (%v, %v), want empty result", got, err)
		}
	})

	t.Run("rejects invalid arguments", func(t *testing.T) {
		if _, err := ReadFromOffset(nil, 0, 1); err == nil {
			t.Error("ReadFromOffset(nil) error = nil, want error")
		}
		if _, err := ReadFromOffset(&PartitionLog{}, 0, 1); err == nil {
			t.Error("ReadFromOffset(empty log) error = nil, want error")
		}
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one"))
		if _, err := ReadFromOffset(log, -1, 1); err == nil {
			t.Error("ReadFromOffset(negative offset) error = nil, want error")
		}
		if _, err := ReadFromOffset(log, 0, -1); err == nil {
			t.Error("ReadFromOffset(negative maximum) error = nil, want error")
		}
	})

	t.Run("skips blank lines", func(t *testing.T) {
		log := openTestPartitionLog(t, "\n"+encodedTestRecord(t, 1, "kept"))
		got, err := ReadFromOffset(log, 0, 2)
		if err != nil {
			t.Fatalf("ReadFromOffset() error = %v", err)
		}
		if len(got) != 1 || got[0].Value != "kept" {
			t.Errorf("ReadFromOffset() = %+v, want one kept record", got)
		}
	})

	t.Run("reports malformed JSON", func(t *testing.T) {
		log := openTestPartitionLog(t, "{\"offset\":0,\"value\":}\n")
		got, err := ReadFromOffset(log, 0, 1)
		if err == nil || got != nil {
			t.Errorf("ReadFromOffset() = (%v, %v), want decode error and nil records", got, err)
		}
	})

	t.Run("reports seek errors", func(t *testing.T) {
		log := openTestPartitionLog(t, encodedTestRecord(t, 0, "one"))
		if err := log.file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFromOffset(log, 0, 1); !errors.Is(err, os.ErrClosed) {
			t.Errorf("ReadFromOffset() error = %v, want closed-file error", err)
		}
	})

	t.Run("reports read errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "write-only.log")
		if err := os.WriteFile(path, []byte("record\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		log := &PartitionLog{file: file, positions: []int64{0}}
		if _, err := ReadFromOffset(log, 0, 1); err == nil {
			t.Fatal("ReadFromOffset() error = nil, want read error")
		}
	})

	t.Run("reports a truncated indexed record", func(t *testing.T) {
		log := openTestPartitionLog(t, "")
		log.positions = []int64{0}
		got, err := ReadFromOffset(log, 0, 1)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("ReadFromOffset() error = nil, want unexpected EOF")
		}
		if got != nil {
			t.Errorf("ReadFromOffset() = %v, want nil records", got)
		}
	})
}

func openTestPartitionLog(t *testing.T, contents string) *PartitionLog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "partition.log")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := OpenPartitionLog(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ClosePartitionLog(log) })
	return log
}

func encodedTestRecord(t *testing.T, offset int64, value string) string {
	t.Helper()
	encoded, err := encodeRecord(Record{Offset: offset, Value: value, Timestamp: 2})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
