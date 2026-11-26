package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Rwatana/miniKafka/internal/storage"
)

func TestBrokerReadKafkaRecordBatches(t *testing.T) {
	t.Run("reads cached batches and high watermark", func(t *testing.T) {
		store := &Broker{DataDir: t.TempDir()}
		for range 2 {
			if _, err := store.AppendKafkaRecordBatch("orders", 0, kafkaBatchForBrokerTest()); err != nil {
				t.Fatal(err)
			}
		}
		batches, highWatermark, err := store.ReadKafkaRecordBatches("orders", 0, 1, 1)
		if err != nil {
			t.Fatalf("ReadKafkaRecordBatches() error = %v", err)
		}
		if len(batches) != 61 || highWatermark != 2 {
			t.Errorf("ReadKafkaRecordBatches() = (%d bytes, %d), want (61 bytes, 2)", len(batches), highWatermark)
		}
	})

	t.Run("opens and recovers existing batches after restart", func(t *testing.T) {
		directory := t.TempDir()
		writer := &Broker{DataDir: directory}
		if _, err := writer.AppendKafkaRecordBatch("orders", 1, kafkaBatchForBrokerTest()); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		reader := &Broker{DataDir: directory}
		batches, highWatermark, err := reader.ReadKafkaRecordBatches("orders", 1, 0, 0)
		if err != nil {
			t.Fatalf("ReadKafkaRecordBatches() error = %v", err)
		}
		if len(batches) != 61 || highWatermark != 1 {
			t.Errorf("ReadKafkaRecordBatches() = (%d bytes, %d), want (61 bytes, 1)", len(batches), highWatermark)
		}
		if len(reader.kafkaLogs) != 1 {
			t.Errorf("cached Kafka logs = %d, want 1", len(reader.kafkaLogs))
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing partition returns empty without creating a file", func(t *testing.T) {
		directory := t.TempDir()
		batches, highWatermark, err := (&Broker{DataDir: directory}).ReadKafkaRecordBatches("new", 3, 0, 1024)
		if err != nil || len(batches) != 0 || highWatermark != 0 {
			t.Errorf("ReadKafkaRecordBatches() = (%v, %d, %v), want (empty, 0, nil)", batches, highWatermark, err)
		}
		path := filepath.Join(directory, "new__3.kafka.log")
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("missing partition stat error = %v, want not-exist", err)
		}
	})

	t.Run("rejects nil broker and invalid arguments", func(t *testing.T) {
		if _, _, err := (*Broker)(nil).ReadKafkaRecordBatches("topic", 0, 0, 1); err == nil {
			t.Error("nil broker error = nil, want validation error")
		}
		for _, test := range []struct {
			name, directory, topic string
			partition              int
			offset                 int64
			maxBytes               int32
		}{
			{name: "empty directory", topic: "topic"},
			{name: "empty topic", directory: t.TempDir()},
			{name: "path topic", directory: t.TempDir(), topic: "../topic"},
			{name: "negative partition", directory: t.TempDir(), topic: "topic", partition: -1},
			{name: "negative fetch offset", directory: t.TempDir(), topic: "topic", offset: -1},
			{name: "negative maximum bytes", directory: t.TempDir(), topic: "topic", maxBytes: -1},
		} {
			t.Run(test.name, func(t *testing.T) {
				if _, _, err := (&Broker{DataDir: test.directory}).ReadKafkaRecordBatches(test.topic, test.partition, test.offset, test.maxBytes); err == nil {
					t.Error("ReadKafkaRecordBatches() error = nil, want validation error")
				}
			})
		}
	})

	t.Run("returns filesystem and recovery errors", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(filePath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := (&Broker{DataDir: filePath}).ReadKafkaRecordBatches("topic", 0, 0, 1024); err == nil {
			t.Error("filesystem error = nil, want error")
		}

		path := filepath.Join(t.TempDir(), "broken__0.kafka.log")
		if err := os.WriteFile(path, []byte("invalid"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := (&Broker{DataDir: filepath.Dir(path)}).ReadKafkaRecordBatches("broken", 0, 0, 1024); err == nil {
			t.Error("recovery error = nil, want malformed-log error")
		}
	})

	t.Run("closes a log handle after recovery fails", func(t *testing.T) {
		originalRecover := recoverBrokerKafkaPartitionLog
		originalClose := closeBrokerKafkaPartitionLog
		closed := false
		recoverBrokerKafkaPartitionLog = func(*storage.KafkaPartitionLog) error {
			return errors.New("recovery failed")
		}
		closeBrokerKafkaPartitionLog = func(log *storage.KafkaPartitionLog) error {
			closed = true
			return originalClose(log)
		}
		defer func() {
			recoverBrokerKafkaPartitionLog = originalRecover
			closeBrokerKafkaPartitionLog = originalClose
		}()

		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "orders__0.kafka.log"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		store := &Broker{DataDir: directory}
		if _, _, err := store.ReadKafkaRecordBatches("orders", 0, 0, 1024); err == nil {
			t.Error("ReadKafkaRecordBatches() error = nil, want recovery error")
		}
		if !closed {
			t.Error("opened log was not closed after recovery failed")
		}
	})

	t.Run("returns an open failure for an existing partition", func(t *testing.T) {
		originalOpen := openBrokerKafkaPartitionLog
		openBrokerKafkaPartitionLog = func(string) (*storage.KafkaPartitionLog, error) {
			return nil, errors.New("open failed")
		}
		defer func() { openBrokerKafkaPartitionLog = originalOpen }()

		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "orders__0.kafka.log"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := (&Broker{DataDir: directory}).ReadKafkaRecordBatches("orders", 0, 0, 1024); err == nil {
			t.Error("ReadKafkaRecordBatches() error = nil, want open error")
		}
	})

	t.Run("returns injected storage errors", func(t *testing.T) {
		originalRead := readBrokerKafkaRecordBatches
		readBrokerKafkaRecordBatches = func(*storage.KafkaPartitionLog, int64, int32) ([]byte, int64, error) {
			return nil, 0, errors.New("read failed")
		}
		defer func() { readBrokerKafkaRecordBatches = originalRead }()

		store := &Broker{
			DataDir:   t.TempDir(),
			kafkaLogs: map[partitionKey]*storage.KafkaPartitionLog{{topic: "broken", partition: 0}: {}},
		}
		if _, _, err := store.ReadKafkaRecordBatches("broken", 0, 0, 1024); err == nil {
			t.Error("injected read error = nil, want error")
		}
	})
}
