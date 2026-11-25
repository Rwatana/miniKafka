package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerProduce(t *testing.T) {
	t.Run("caches topic partition logs and recovers after restart", func(t *testing.T) {
		dataDir := t.TempDir()
		broker := &Broker{DataDir: dataDir}
		key := "customer-42"
		first, err := broker.Produce("orders", 0, &key, "created")
		if err != nil || first != 0 {
			t.Fatalf("first Produce() = (%d, %v), want (0, nil)", first, err)
		}
		second, err := broker.Produce("orders", 0, nil, "updated")
		if err != nil || second != 1 {
			t.Fatalf("second Produce() = (%d, %v), want (1, nil)", second, err)
		}
		otherPartition, err := broker.Produce("orders", 1, nil, "other-partition")
		if err != nil || otherPartition != 0 {
			t.Fatalf("other partition Produce() = (%d, %v), want (0, nil)", otherPartition, err)
		}
		if len(broker.logs) != 2 {
			t.Errorf("cached logs = %d, want 2", len(broker.logs))
		}
		closeBrokerLogs(t, broker)

		restarted := &Broker{DataDir: dataDir}
		afterRestart, err := restarted.Produce("orders", 0, nil, "recovered")
		if err != nil || afterRestart != 2 {
			t.Errorf("Produce() after restart = (%d, %v), want (2, nil)", afterRestart, err)
		}
		closeBrokerLogs(t, restarted)
	})

	t.Run("serializes concurrent produces", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		const count = 32
		offsets := make(chan int64, count)
		errorsFound := make(chan error, count)
		var wait sync.WaitGroup
		for index := 0; index < count; index++ {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				offset, err := broker.Produce("events", 0, nil, fmt.Sprintf("event-%d", index))
				if err != nil {
					errorsFound <- err
					return
				}
				offsets <- offset
			}(index)
		}
		wait.Wait()
		close(offsets)
		close(errorsFound)
		for err := range errorsFound {
			t.Errorf("Produce() error = %v", err)
		}
		seen := make(map[int64]bool, count)
		for offset := range offsets {
			if offset < 0 || offset >= count || seen[offset] {
				t.Errorf("invalid or duplicate offset %d", offset)
			}
			seen[offset] = true
		}
		if len(seen) != count {
			t.Errorf("unique offsets = %d, want %d", len(seen), count)
		}
		closeBrokerLogs(t, broker)
	})

	t.Run("rejects invalid broker inputs", func(t *testing.T) {
		if _, err := (*Broker)(nil).Produce("orders", 0, nil, "value"); err == nil {
			t.Error("nil broker error = nil, want error")
		}
		broker := &Broker{}
		if _, err := broker.Produce("orders", 0, nil, "value"); err == nil {
			t.Error("empty data directory error = nil, want error")
		}
		broker.DataDir = t.TempDir()
		for _, topic := range []string{"", "../escape", `bad\topic`} {
			if _, err := broker.Produce(topic, 0, nil, "value"); err == nil {
				t.Errorf("Produce(%q) error = nil, want invalid topic error", topic)
			}
		}
		if _, err := broker.Produce("orders", -1, nil, "value"); err == nil {
			t.Error("negative partition error = nil, want error")
		}
	})

	t.Run("reports data directory and open failures", func(t *testing.T) {
		root := t.TempDir()
		fileAsDirectory := filepath.Join(root, "not-a-directory")
		if err := os.WriteFile(fileAsDirectory, []byte("file"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: fileAsDirectory}).Produce("orders", 0, nil, "value"); err == nil {
			t.Error("MkdirAll failure error = nil, want error")
		}

		dataDir := filepath.Join(root, "logs")
		if err := os.MkdirAll(filepath.Join(dataDir, "orders__0.log"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: dataDir}).Produce("orders", 0, nil, "value"); err == nil {
			t.Error("opening a directory as a log error = nil, want error")
		}
	})

	t.Run("reports append failures", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		originalAppend := appendBrokerRecord
		appendBrokerRecord = func(*storage.PartitionLog, *string, string) (int64, error) {
			return 0, errors.New("append failed")
		}
		defer func() { appendBrokerRecord = originalAppend }()
		if _, err := broker.Produce("orders", 0, nil, "value"); err == nil {
			t.Error("Produce() error = nil, want append error")
		}
		closeBrokerLogs(t, broker)
	})
}

func closeBrokerLogs(t *testing.T, broker *Broker) {
	t.Helper()
	for _, log := range broker.logs {
		if err := storage.ClosePartitionLog(log); err != nil {
			t.Errorf("ClosePartitionLog() error = %v", err)
		}
	}
}
