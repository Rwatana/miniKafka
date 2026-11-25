package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerProduceBatch(t *testing.T) {
	t.Run("writes and caches one partition batch", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		key := "order-7"
		offsets, err := broker.ProduceBatch("orders", 0, []storage.Record{{Key: &key, Value: "created"}, {Value: "paid"}})
		if err != nil {
			t.Fatalf("ProduceBatch() error = %v", err)
		}
		if !reflect.DeepEqual(offsets, []int64{0, 1}) {
			t.Fatalf("offsets = %v, want [0 1]", offsets)
		}
		if len(broker.logs) != 1 {
			t.Errorf("cached logs = %d, want one", len(broker.logs))
		}
		records, err := broker.Consume("orders", 0, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 || records[0].Value != "created" || records[1].Value != "paid" {
			t.Errorf("records = %+v, want created then paid", records)
		}
		closeBrokerLogs(t, broker)
	})

	t.Run("empty batch does not create a partition log", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "not-created")
		broker := &Broker{DataDir: dataDir}
		offsets, err := broker.ProduceBatch("orders", 0, nil)
		if err != nil || len(offsets) != 0 {
			t.Errorf("ProduceBatch(empty) = (%v, %v), want empty offsets and no error", offsets, err)
		}
		if _, err := os.Stat(dataDir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("data directory stat error = %v, want not-exist", err)
		}
	})

	t.Run("rejects invalid inputs", func(t *testing.T) {
		if _, err := (*Broker)(nil).ProduceBatch("orders", 0, nil); err == nil {
			t.Error("nil broker error = nil, want error")
		}
		broker := &Broker{}
		if _, err := broker.ProduceBatch("orders", 0, nil); err == nil {
			t.Error("empty data directory error = nil, want error")
		}
		broker.DataDir = t.TempDir()
		for _, topic := range []string{"", "../escape", `bad\topic`} {
			if _, err := broker.ProduceBatch(topic, 0, nil); err == nil {
				t.Errorf("ProduceBatch(%q) error = nil, want invalid-topic error", topic)
			}
		}
		if _, err := broker.ProduceBatch("orders", -1, nil); err == nil {
			t.Error("negative partition error = nil, want error")
		}
	})

	t.Run("reports directory and open failures", func(t *testing.T) {
		root := t.TempDir()
		fileAsDirectory := filepath.Join(root, "not-a-directory")
		if err := os.WriteFile(fileAsDirectory, []byte("file"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: fileAsDirectory}).ProduceBatch("orders", 0, []storage.Record{{Value: "x"}}); err == nil {
			t.Error("MkdirAll failure error = nil, want error")
		}
		dataDir := filepath.Join(root, "logs")
		if err := os.MkdirAll(filepath.Join(dataDir, "orders__0.log"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: dataDir}).ProduceBatch("orders", 0, []storage.Record{{Value: "x"}}); err == nil {
			t.Error("opening a directory as a log error = nil, want error")
		}
	})

	t.Run("reports batch append failures", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		originalAppend := appendBrokerRecords
		appendBrokerRecords = func(*storage.PartitionLog, []storage.Record) ([]int64, error) {
			return nil, errors.New("batch append failed")
		}
		defer func() { appendBrokerRecords = originalAppend }()
		if _, err := broker.ProduceBatch("orders", 0, []storage.Record{{Value: "x"}}); err == nil {
			t.Error("ProduceBatch() error = nil, want append error")
		}
		closeBrokerLogs(t, broker)
	})

	t.Run("serializes concurrent batches", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		const batches = 8
		offsets := make(chan int64, batches*2)
		errorsFound := make(chan error, batches)
		var wait sync.WaitGroup
		for index := 0; index < batches; index++ {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				assigned, err := broker.ProduceBatch("events", 0, []storage.Record{{Value: fmt.Sprintf("%d-a", index)}, {Value: fmt.Sprintf("%d-b", index)}})
				if err != nil {
					errorsFound <- err
					return
				}
				for _, offset := range assigned {
					offsets <- offset
				}
			}(index)
		}
		wait.Wait()
		close(offsets)
		close(errorsFound)
		for err := range errorsFound {
			t.Errorf("ProduceBatch() error = %v", err)
		}
		seen := make(map[int64]bool, batches*2)
		for offset := range offsets {
			if offset < 0 || offset >= batches*2 || seen[offset] {
				t.Errorf("invalid or duplicate offset %d", offset)
			}
			seen[offset] = true
		}
		if len(seen) != batches*2 {
			t.Errorf("unique offsets = %d, want %d", len(seen), batches*2)
		}
		closeBrokerLogs(t, broker)
	})
}
