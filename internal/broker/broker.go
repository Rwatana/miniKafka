package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Rwatana/miniKafka/internal/storage"
)

var appendBrokerRecord = storage.AppendRecord

type partitionKey struct {
	topic     string
	partition int
}

// Broker owns the data directory and open partition logs for one broker.
type Broker struct {
	DataDir string
	mu      sync.Mutex
	logs    map[partitionKey]*storage.PartitionLog
}

// Produce appends a record to the selected topic partition and returns its offset.
func (broker *Broker) Produce(topic string, partition int, key *string, value string) (int64, error) {
	if broker == nil {
		return 0, errors.New("produce record: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return 0, errors.New("produce record: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\`) {
		return 0, fmt.Errorf("produce record: invalid topic %q", topic)
	}
	if partition < 0 {
		return 0, fmt.Errorf("produce record: negative partition %d", partition)
	}

	if broker.logs == nil {
		broker.logs = make(map[partitionKey]*storage.PartitionLog)
	}
	identity := partitionKey{topic: topic, partition: partition}
	log := broker.logs[identity]
	if log == nil {
		if err := os.MkdirAll(broker.DataDir, 0o700); err != nil {
			return 0, fmt.Errorf("create broker data directory: %w", err)
		}
		filename := fmt.Sprintf("%s__%d.log", topic, partition)
		openedLog, err := storage.OpenPartitionLog(filepath.Join(broker.DataDir, filename))
		if err != nil {
			return 0, fmt.Errorf("open topic %q partition %d: %w", topic, partition, err)
		}
		log = openedLog
		broker.logs[identity] = log
	}

	offset, err := appendBrokerRecord(log, key, value)
	if err != nil {
		return 0, fmt.Errorf("produce topic %q partition %d: %w", topic, partition, err)
	}
	return offset, nil
}
