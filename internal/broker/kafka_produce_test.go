package broker

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerAppendKafkaRecordBatch(t *testing.T) {
	t.Run("caches and recovers partition offsets", func(t *testing.T) {
		dataDir := t.TempDir()
		broker := &Broker{DataDir: dataDir}
		first, err := broker.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest())
		if err != nil || first != 0 {
			t.Fatalf("first append = (%d, %v), want (0, nil)", first, err)
		}
		second, err := broker.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest())
		if err != nil || second != 1 {
			t.Fatalf("second append = (%d, %v), want (1, nil)", second, err)
		}
		if len(broker.kafkaLogs) != 1 {
			t.Errorf("cached Kafka logs = %d, want 1", len(broker.kafkaLogs))
		}
		if err := closeBrokerKafkaPartitionLog(broker.kafkaLogs[partitionKey{topic: "orders", partition: 0}]); err != nil {
			t.Fatal(err)
		}
		restarted := &Broker{DataDir: dataDir}
		third, err := restarted.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest())
		if err != nil || third != 2 {
			t.Fatalf("append after restart = (%d, %v), want (2, nil)", third, err)
		}
		closeBrokerKafkaLogsForTest(t, restarted)
	})

	t.Run("serializes concurrent writes", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		const writes = 8
		var wait sync.WaitGroup
		bases := make(chan int64, writes)
		errorsFound := make(chan error, writes)
		for index := 0; index < writes; index++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				base, err := broker.AppendKafkaRecordBatch("events", 0, kafkaBatchForBrokerTest())
				if err != nil {
					errorsFound <- err
					return
				}
				bases <- base
			}()
		}
		wait.Wait()
		close(bases)
		close(errorsFound)
		for err := range errorsFound {
			t.Errorf("AppendKafkaRecordBatch() error = %v", err)
		}
		seen := make(map[int64]bool, writes)
		for base := range bases {
			if base < 0 || base >= writes || seen[base] {
				t.Errorf("invalid or duplicate base offset %d", base)
			}
			seen[base] = true
		}
		if len(seen) != writes {
			t.Errorf("unique base offsets = %d, want %d", len(seen), writes)
		}
		closeBrokerKafkaLogsForTest(t, broker)
	})
}

func TestBrokerAppendKafkaRecordBatchRejectsInvalidInputs(t *testing.T) {
	if _, err := (*Broker)(nil).AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
		t.Error("nil broker error = nil, want error")
	}
	broker := &Broker{}
	if _, err := broker.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
		t.Error("empty data directory error = nil, want error")
	}
	broker.DataDir = t.TempDir()
	for _, topic := range []string{"", "../escape", `bad\topic`} {
		if _, err := broker.AppendKafkaRecordBatch(topic, 0, kafkaBatchForBrokerTest()); err == nil {
			t.Errorf("invalid topic %q error = nil, want error", topic)
		}
	}
	if _, err := broker.AppendKafkaRecordBatch("orders", -1, kafkaBatchForBrokerTest()); err == nil {
		t.Error("negative partition error = nil, want error")
	}
}

func TestBrokerAppendKafkaRecordBatchReportsStorageFailures(t *testing.T) {
	t.Run("directory failure", func(t *testing.T) {
		root := t.TempDir()
		dataDir := filepath.Join(root, "not-a-directory")
		if err := os.WriteFile(dataDir, []byte("file"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: dataDir}).AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
			t.Error("AppendKafkaRecordBatch() error = nil, want directory error")
		}
	})

	t.Run("open failure", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "orders__0.kafka.log"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: root}).AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
			t.Error("AppendKafkaRecordBatch() error = nil, want open error")
		}
	})

	t.Run("open and recovery injected failures", func(t *testing.T) {
		originalOpen := openBrokerKafkaPartitionLog
		originalRecover := recoverBrokerKafkaPartitionLog
		openBrokerKafkaPartitionLog = func(string) (*storage.KafkaPartitionLog, error) { return nil, errors.New("open failed") }
		if _, err := (&Broker{DataDir: t.TempDir()}).AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
			t.Error("injected open error = nil, want error")
		}
		openBrokerKafkaPartitionLog = originalOpen
		recoverBrokerKafkaPartitionLog = func(*storage.KafkaPartitionLog) error { return errors.New("recovery failed") }
		broker := &Broker{DataDir: t.TempDir()}
		if _, err := broker.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
			t.Error("injected recovery error = nil, want error")
		}
		if len(broker.kafkaLogs) != 0 {
			t.Errorf("cached logs after recovery failure = %d, want zero", len(broker.kafkaLogs))
		}
		openBrokerKafkaPartitionLog = originalOpen
		recoverBrokerKafkaPartitionLog = originalRecover
	})

	t.Run("append failure", func(t *testing.T) {
		originalAppend := appendBrokerKafkaRecordBatch
		appendBrokerKafkaRecordBatch = func(*storage.KafkaPartitionLog, []byte) (int64, error) { return 0, errors.New("append failed") }
		defer func() { appendBrokerKafkaRecordBatch = originalAppend }()
		broker := &Broker{DataDir: t.TempDir()}
		if _, err := broker.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err == nil {
			t.Error("injected append error = nil, want error")
		}
		closeBrokerKafkaLogsForTest(t, broker)
	})
}

func closeBrokerKafkaLogsForTest(t *testing.T, broker *Broker) {
	t.Helper()
	for _, log := range broker.kafkaLogs {
		if err := closeBrokerKafkaPartitionLog(log); err != nil {
			t.Errorf("close Kafka log: %v", err)
		}
	}
}

func kafkaBatchForBrokerTest() []byte {
	batch := make([]byte, 61)
	binary.BigEndian.PutUint32(batch[8:12], 49)
	batch[16] = 2
	binary.BigEndian.PutUint32(batch[57:61], 1)
	checksum := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], checksum)
	return batch
}
