package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenKafkaPartitionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "partition.log")
	log, err := OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
	}
	if log.file == nil || log.nextOffset != 0 {
		t.Errorf("opened log = %+v, want open file and initial offset zero", log)
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close new log: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat new log: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("log permissions = %o, want 600", info.Mode().Perm())
	}
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatalf("write existing log: %v", err)
	}
	log, err = OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog(existing) error = %v", err)
	}
	if _, err := log.file.Write([]byte("+append")); err != nil {
		t.Fatalf("append to opened log: %v", err)
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close existing log: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read appended log: %v", err)
	}
	if string(contents) != "existing+append" {
		t.Errorf("log contents = %q, want %q", contents, "existing+append")
	}
}

func TestOpenKafkaPartitionLogRejectsInvalidPaths(t *testing.T) {
	if log, err := OpenKafkaPartitionLog(""); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(empty) = (%v, %v), want empty-path error", log, err)
	}
	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("file"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	if log, err := OpenKafkaPartitionLog(filepath.Join(parentFile, "partition.log")); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(blocked parent) = (%v, %v), want directory error", log, err)
	}
	directoryPath := t.TempDir()
	if log, err := OpenKafkaPartitionLog(directoryPath); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(directory) = (%v, %v), want open error", log, err)
	}
}
