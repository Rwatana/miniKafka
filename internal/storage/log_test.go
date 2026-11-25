package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestOpenPartitionLog(t *testing.T) {
	t.Run("creates an empty log", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "topic__0.log")
		log, err := openPartitionLog(path)
		if err != nil {
			t.Fatalf("openPartitionLog() error = %v", err)
		}
		if log.file == nil {
			t.Fatal("openPartitionLog() returned a nil file")
		}
		defer log.file.Close()
		if len(log.positions) != 0 {
			t.Errorf("positions = %v, want empty index", log.positions)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("partition log was not created: %v", err)
		}
	})

	t.Run("recovers existing positions and preserves data", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "topic__1.log")
		original := []byte("first\nsecond\n")
		if err := os.WriteFile(path, original, 0o600); err != nil {
			t.Fatal(err)
		}
		log, err := openPartitionLog(path)
		if err != nil {
			t.Fatalf("openPartitionLog() error = %v", err)
		}
		if !reflect.DeepEqual(log.positions, []int64{0, 6}) {
			t.Errorf("positions = %v, want [0 6]", log.positions)
		}
		if _, err := log.file.Write([]byte("third\n")); err != nil {
			t.Fatalf("append after recovery: %v", err)
		}
		if err := log.file.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := append(original, []byte("third\n")...)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("file contents = %q, want %q", got, want)
		}
	})

	t.Run("open failure", func(t *testing.T) {
		originalOpenFile := partitionLogOpenFile
		partitionLogOpenFile = func(string, int, os.FileMode) (*os.File, error) {
			return nil, os.ErrPermission
		}
		defer func() { partitionLogOpenFile = originalOpenFile }()

		log, err := openPartitionLog("ignored.log")
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("openPartitionLog() error = %v, want permission error", err)
		}
		if log != nil {
			t.Errorf("openPartitionLog() = %+v, want nil log", log)
		}
	})

	t.Run("recovery failure closes the file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "write-only.log")
		originalOpenFile := partitionLogOpenFile
		var openedFile *os.File
		partitionLogOpenFile = func(string, int, os.FileMode) (*os.File, error) {
			file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
			openedFile = file
			return file, err
		}
		defer func() { partitionLogOpenFile = originalOpenFile }()

		log, err := openPartitionLog("ignored.log")
		if err == nil {
			t.Fatal("openPartitionLog() error = nil, want recovery error")
		}
		if log != nil {
			t.Errorf("openPartitionLog() = %+v, want nil log", log)
		}
		if _, statErr := openedFile.Stat(); !errors.Is(statErr, os.ErrClosed) {
			t.Errorf("file stat error = %v, want closed file", statErr)
		}
	})
}

func TestClosePartitionLog(t *testing.T) {
	t.Run("nil log", func(t *testing.T) {
		if err := closePartitionLog(nil); err != nil {
			t.Errorf("closePartitionLog(nil) error = %v, want nil", err)
		}
	})

	t.Run("nil file", func(t *testing.T) {
		if err := closePartitionLog(&partitionLog{}); err != nil {
			t.Errorf("closePartitionLog(empty log) error = %v, want nil", err)
		}
	})

	t.Run("closes the owned file", func(t *testing.T) {
		log, err := openPartitionLog(filepath.Join(t.TempDir(), "close.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := closePartitionLog(log); err != nil {
			t.Fatalf("closePartitionLog() error = %v", err)
		}
		if _, err := log.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("file stat error = %v, want closed file", err)
		}
	})

	t.Run("reports an underlying close error", func(t *testing.T) {
		log, err := openPartitionLog(filepath.Join(t.TempDir(), "repeat-close.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := log.file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := closePartitionLog(log); !errors.Is(err, os.ErrClosed) {
			t.Errorf("closePartitionLog() error = %v, want closed-file error", err)
		}
	})
}
