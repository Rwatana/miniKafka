package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerLatestOffset(t *testing.T) {
	t.Run("returns next offset from a cached log", func(t *testing.T) {
		store := &Broker{DataDir: t.TempDir()}
		if _, err := store.Produce("orders", 2, nil, "created"); err != nil {
			t.Fatal(err)
		}
		offset, err := store.LatestOffset("orders", 2)
		if err != nil || offset != 1 {
			t.Errorf("LatestOffset() = (%d, %v), want (1, nil)", offset, err)
		}
	})

	t.Run("recovers an existing log after restart", func(t *testing.T) {
		directory := t.TempDir()
		writer := &Broker{DataDir: directory}
		if _, err := writer.Produce("orders", 0, nil, "first"); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Produce("orders", 0, nil, "second"); err != nil {
			t.Fatal(err)
		}
		offset, err := (&Broker{DataDir: directory}).LatestOffset("orders", 0)
		if err != nil || offset != 2 {
			t.Errorf("LatestOffset() = (%d, %v), want (2, nil)", offset, err)
		}
	})

	t.Run("returns zero without creating a missing partition", func(t *testing.T) {
		directory := t.TempDir()
		offset, err := (&Broker{DataDir: directory}).LatestOffset("new-topic", 4)
		if err != nil || offset != 0 {
			t.Errorf("LatestOffset() = (%d, %v), want (0, nil)", offset, err)
		}
		if _, err := os.Stat(filepath.Join(directory, "new-topic__4.log")); !os.IsNotExist(err) {
			t.Errorf("missing partition stat error = %v, want no log file", err)
		}
	})

	t.Run("rejects nil broker and invalid arguments", func(t *testing.T) {
		if _, err := (*Broker)(nil).LatestOffset("topic", 0); err == nil {
			t.Error("LatestOffset(nil broker) error = nil, want error")
		}
		for _, test := range []struct {
			name  string
			data  string
			topic string
			part  int
		}{
			{name: "empty directory", topic: "topic"},
			{name: "empty topic", data: t.TempDir()},
			{name: "path topic", data: t.TempDir(), topic: "../topic"},
			{name: "negative partition", data: t.TempDir(), topic: "topic", part: -1},
		} {
			t.Run(test.name, func(t *testing.T) {
				if _, err := (&Broker{DataDir: test.data}).LatestOffset(test.topic, test.part); err == nil {
					t.Error("LatestOffset() error = nil, want validation error")
				}
			})
		}
	})

	t.Run("returns filesystem and recovery errors", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(filePath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: filePath}).LatestOffset("topic", 0); err == nil {
			t.Error("LatestOffset() error = nil, want filesystem error")
		}

		originalOpen := openBrokerPartitionLog
		openBrokerPartitionLog = func(string) (*storage.PartitionLog, error) { return nil, errors.New("open failed") }
		defer func() { openBrokerPartitionLog = originalOpen }()
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "broken__0.log"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: directory}).LatestOffset("broken", 0); err == nil {
			t.Error("LatestOffset() error = nil, want log open error")
		}
	})

	t.Run("returns cached storage errors", func(t *testing.T) {
		store := &Broker{
			DataDir: t.TempDir(),
			logs:    map[partitionKey]*storage.PartitionLog{{topic: "broken", partition: 0}: {}},
		}
		if _, err := store.LatestOffset("broken", 0); err == nil {
			t.Error("LatestOffset() error = nil, want cached storage error")
		}
	})
}
