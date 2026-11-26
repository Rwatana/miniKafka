package broker

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestListPartitions(t *testing.T) {
	t.Run("discovers log files and skips invalid entries", func(t *testing.T) {
		directory := t.TempDir()
		for _, filename := range []string{"orders__2.log", "orders__0.log", "events__1.log", "events__1.kafka.log", "kafka-only__0.kafka.log", "bad.log", "bad__x.log", "bad__-1.log", "bad__x.kafka.log", "notes.txt", "__4.log"} {
			if err := os.WriteFile(filepath.Join(directory, filename), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Mkdir(filepath.Join(directory, "nested__7.log"), 0o700); err != nil {
			t.Fatal(err)
		}
		got, err := (&Broker{DataDir: directory}).ListPartitions()
		if err != nil {
			t.Fatalf("ListPartitions() error = %v", err)
		}
		want := []PartitionInfo{{Topic: "events", Partition: 1}, {Topic: "kafka-only", Partition: 0}, {Topic: "orders", Partition: 0}, {Topic: "orders", Partition: 2}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ListPartitions() = %+v, want %+v", got, want)
		}
	})

	t.Run("creates a missing data directory and returns no partitions", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "new-data")
		got, err := (&Broker{DataDir: directory}).ListPartitions()
		if err != nil {
			t.Fatalf("ListPartitions() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("ListPartitions() = %+v, want empty result", got)
		}
		if info, err := os.Stat(directory); err != nil || !info.IsDir() {
			t.Errorf("data directory stat = (%v, %v), want existing directory", info, err)
		}
	})

	t.Run("rejects nil broker", func(t *testing.T) {
		if _, err := (*Broker)(nil).ListPartitions(); err == nil {
			t.Fatal("ListPartitions() error = nil, want nil broker error")
		}
	})

	t.Run("rejects empty data directory", func(t *testing.T) {
		if _, err := (&Broker{}).ListPartitions(); err == nil {
			t.Fatal("ListPartitions() error = nil, want empty directory error")
		}
	})

	t.Run("returns directory creation errors", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(filePath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: filePath}).ListPartitions(); err == nil {
			t.Fatal("ListPartitions() error = nil, want directory creation error")
		}
	})

	t.Run("returns directory read errors", func(t *testing.T) {
		originalReadDirectory := readBrokerDirectory
		readBrokerDirectory = func(string) ([]os.DirEntry, error) { return nil, errors.New("read failed") }
		defer func() { readBrokerDirectory = originalReadDirectory }()
		if _, err := (&Broker{DataDir: t.TempDir()}).ListPartitions(); err == nil {
			t.Fatal("ListPartitions() error = nil, want read error")
		}
	})
}
