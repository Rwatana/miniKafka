package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerClose(t *testing.T) {
	t.Run("closes all cached logs and clears the cache", func(t *testing.T) {
		store := &Broker{DataDir: t.TempDir()}
		if _, err := store.Produce("orders", 0, nil, "first"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Produce("orders", 2, nil, "second"); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if len(store.logs) != 0 {
			t.Errorf("cached logs = %d, want 0 after close", len(store.logs))
		}
		if err := store.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
	})

	t.Run("closes healthy logs even when another close fails", func(t *testing.T) {
		directory := t.TempDir()
		failedLog, err := storage.OpenPartitionLog(filepath.Join(directory, "broken__0.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := storage.ClosePartitionLog(failedLog); err != nil {
			t.Fatal(err)
		}
		healthyLog, err := storage.OpenPartitionLog(filepath.Join(directory, "healthy__0.log"))
		if err != nil {
			t.Fatal(err)
		}
		store := &Broker{logs: map[partitionKey]*storage.PartitionLog{
			{topic: "broken", partition: 0}:  failedLog,
			{topic: "healthy", partition: 0}: healthyLog,
		}}
		err = store.Close()
		if err == nil || !errors.Is(err, os.ErrClosed) {
			t.Errorf("Close() error = %v, want closed-log error", err)
		}
		if len(store.logs) != 0 {
			t.Errorf("cached logs = %d, want 0 after close", len(store.logs))
		}
		if _, err := storage.AppendRecord(healthyLog, nil, "closed"); err == nil {
			t.Error("healthy log remained open after another log failed to close")
		}
	})

	t.Run("nil broker and empty cache are safe", func(t *testing.T) {
		if err := (*Broker)(nil).Close(); err != nil {
			t.Errorf("Close(nil) error = %v, want nil", err)
		}
		if err := (&Broker{}).Close(); err != nil {
			t.Errorf("Close(empty broker) error = %v, want nil", err)
		}
	})
}
