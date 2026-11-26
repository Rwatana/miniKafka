package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Rwatana/miniKafka/internal/storage"
)

var appendBrokerRecord = storage.AppendRecord
var appendBrokerRecords = storage.AppendRecords
var readBrokerDirectory = os.ReadDir
var openBrokerPartitionLog = storage.OpenPartitionLog
var openBrokerKafkaPartitionLog = storage.OpenKafkaPartitionLog
var recoverBrokerKafkaPartitionLog = storage.RecoverKafkaPartitionLog
var appendBrokerKafkaRecordBatch = storage.AppendKafkaRecordBatch
var readBrokerKafkaRecordBatches = storage.ReadKafkaRecordBatches
var closeBrokerKafkaPartitionLog = storage.CloseKafkaPartitionLog
var statBrokerKafkaPartitionLog = os.Stat

// PartitionInfo identifies one topic partition found in the broker data directory.
type PartitionInfo struct {
	Topic     string
	Partition int
}

type partitionKey struct {
	topic     string
	partition int
}

// Broker owns the data directory and open partition logs for one broker.
type Broker struct {
	DataDir   string
	mu        sync.Mutex
	logs      map[partitionKey]*storage.PartitionLog
	kafkaLogs map[partitionKey]*storage.KafkaPartitionLog
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

// ProduceBatch appends records to one topic partition and returns their offsets.
func (broker *Broker) ProduceBatch(topic string, partition int, records []storage.Record) ([]int64, error) {
	if broker == nil {
		return nil, errors.New("produce record batch: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return nil, errors.New("produce record batch: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\\`) {
		return nil, fmt.Errorf("produce record batch: invalid topic %q", topic)
	}
	if partition < 0 {
		return nil, fmt.Errorf("produce record batch: negative partition %d", partition)
	}
	if len(records) == 0 {
		return []int64{}, nil
	}

	if broker.logs == nil {
		broker.logs = make(map[partitionKey]*storage.PartitionLog)
	}
	identity := partitionKey{topic: topic, partition: partition}
	log := broker.logs[identity]
	if log == nil {
		if err := os.MkdirAll(broker.DataDir, 0o700); err != nil {
			return nil, fmt.Errorf("create broker data directory: %w", err)
		}
		filename := fmt.Sprintf("%s__%d.log", topic, partition)
		openedLog, err := storage.OpenPartitionLog(filepath.Join(broker.DataDir, filename))
		if err != nil {
			return nil, fmt.Errorf("open topic %q partition %d: %w", topic, partition, err)
		}
		log = openedLog
		broker.logs[identity] = log
	}

	offsets, err := appendBrokerRecords(log, records)
	if err != nil {
		return nil, fmt.Errorf("produce topic %q partition %d batch: %w", topic, partition, err)
	}
	return offsets, nil
}

// AppendKafkaRecordBatch appends a validated Kafka batch to one broker-owned topic partition.
func (broker *Broker) AppendKafkaRecordBatch(topic string, partition int, batch []byte) (int64, error) {
	if broker == nil {
		return 0, errors.New("append Kafka record batch: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return 0, errors.New("append Kafka record batch: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\\`) {
		return 0, fmt.Errorf("append Kafka record batch: invalid topic %q", topic)
	}
	if partition < 0 {
		return 0, fmt.Errorf("append Kafka record batch: negative partition %d", partition)
	}
	if broker.kafkaLogs == nil {
		broker.kafkaLogs = make(map[partitionKey]*storage.KafkaPartitionLog)
	}
	identity := partitionKey{topic: topic, partition: partition}
	log := broker.kafkaLogs[identity]
	if log == nil {
		filename := fmt.Sprintf("%s__%d.kafka.log", topic, partition)
		openedLog, err := openBrokerKafkaPartitionLog(filepath.Join(broker.DataDir, filename))
		if err != nil {
			return 0, fmt.Errorf("open Kafka topic %q partition %d: %w", topic, partition, err)
		}
		if err := recoverBrokerKafkaPartitionLog(openedLog); err != nil {
			_ = closeBrokerKafkaPartitionLog(openedLog)
			return 0, fmt.Errorf("recover Kafka topic %q partition %d: %w", topic, partition, err)
		}
		log = openedLog
		broker.kafkaLogs[identity] = log
	}
	baseOffset, err := appendBrokerKafkaRecordBatch(log, batch)
	if err != nil {
		return 0, fmt.Errorf("append Kafka topic %q partition %d: %w", topic, partition, err)
	}
	return baseOffset, nil
}

// ReadKafkaRecordBatches reads whole batches at or after an offset and returns the partition high watermark.
func (broker *Broker) ReadKafkaRecordBatches(topic string, partition int, fetchOffset int64, maxBytes int32) ([]byte, int64, error) {
	if broker == nil {
		return nil, 0, errors.New("read Kafka record batches: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return nil, 0, errors.New("read Kafka record batches: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\\`) {
		return nil, 0, fmt.Errorf("read Kafka record batches: invalid topic %q", topic)
	}
	if partition < 0 {
		return nil, 0, fmt.Errorf("read Kafka record batches: negative partition %d", partition)
	}
	if fetchOffset < 0 {
		return nil, 0, fmt.Errorf("read Kafka record batches: negative fetch offset %d", fetchOffset)
	}
	if maxBytes < 0 {
		return nil, 0, fmt.Errorf("read Kafka record batches: negative maximum bytes %d", maxBytes)
	}

	identity := partitionKey{topic: topic, partition: partition}
	log := broker.kafkaLogs[identity]
	if log == nil {
		path := filepath.Join(broker.DataDir, fmt.Sprintf("%s__%d.kafka.log", topic, partition))
		if _, err := statBrokerKafkaPartitionLog(path); errors.Is(err, os.ErrNotExist) {
			return []byte{}, 0, nil
		} else if err != nil {
			return nil, 0, fmt.Errorf("find Kafka topic %q partition %d: %w", topic, partition, err)
		}
		openedLog, err := openBrokerKafkaPartitionLog(path)
		if err != nil {
			return nil, 0, fmt.Errorf("open Kafka topic %q partition %d: %w", topic, partition, err)
		}
		if err := recoverBrokerKafkaPartitionLog(openedLog); err != nil {
			_ = closeBrokerKafkaPartitionLog(openedLog)
			return nil, 0, fmt.Errorf("recover Kafka topic %q partition %d: %w", topic, partition, err)
		}
		if broker.kafkaLogs == nil {
			broker.kafkaLogs = make(map[partitionKey]*storage.KafkaPartitionLog)
		}
		log = openedLog
		broker.kafkaLogs[identity] = log
	}

	records, highWatermark, err := readBrokerKafkaRecordBatches(log, fetchOffset, maxBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("read Kafka topic %q partition %d: %w", topic, partition, err)
	}
	return records, highWatermark, nil
}

// Consume reads a bounded range of records from a topic partition.
func (broker *Broker) Consume(topic string, partition int, fromOffset int64, max int) ([]storage.Record, error) {
	if broker == nil {
		return nil, errors.New("consume records: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return nil, errors.New("consume records: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\`) {
		return nil, fmt.Errorf("consume records: invalid topic %q", topic)
	}
	if partition < 0 {
		return nil, fmt.Errorf("consume records: negative partition %d", partition)
	}
	if fromOffset < 0 {
		return nil, fmt.Errorf("consume records: negative offset %d", fromOffset)
	}
	if max < 0 {
		return nil, fmt.Errorf("consume records: negative maximum %d", max)
	}
	if max == 0 {
		return []storage.Record{}, nil
	}

	identity := partitionKey{topic: topic, partition: partition}
	log := broker.logs[identity]
	if log == nil {
		filename := fmt.Sprintf("%s__%d.log", topic, partition)
		path := filepath.Join(broker.DataDir, filename)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return []storage.Record{}, nil
		} else if err != nil {
			return nil, fmt.Errorf("find topic %q partition %d: %w", topic, partition, err)
		}
		openedLog, err := storage.OpenPartitionLog(path)
		if err != nil {
			return nil, fmt.Errorf("open topic %q partition %d: %w", topic, partition, err)
		}
		if broker.logs == nil {
			broker.logs = make(map[partitionKey]*storage.PartitionLog)
		}
		log = openedLog
		broker.logs[identity] = log
	}

	records, err := storage.ReadFromOffset(log, fromOffset, max)
	if err != nil {
		return nil, fmt.Errorf("consume topic %q partition %d: %w", topic, partition, err)
	}
	return records, nil
}

// ListPartitions discovers existing topic partitions from their log filenames.
func (broker *Broker) ListPartitions() ([]PartitionInfo, error) {
	if broker == nil {
		return nil, errors.New("list topic partitions: nil broker")
	}
	if broker.DataDir == "" {
		return nil, errors.New("list topic partitions: empty data directory")
	}
	if err := os.MkdirAll(broker.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create broker data directory: %w", err)
	}
	entries, err := readBrokerDirectory(broker.DataDir)
	if err != nil {
		return nil, fmt.Errorf("read broker data directory: %w", err)
	}

	partitions := make([]PartitionInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		stem := strings.TrimSuffix(entry.Name(), ".log")
		separator := strings.LastIndex(stem, "__")
		if separator <= 0 {
			continue
		}
		partition, err := strconv.Atoi(stem[separator+2:])
		if err != nil || partition < 0 {
			continue
		}
		partitions = append(partitions, PartitionInfo{Topic: stem[:separator], Partition: partition})
	}
	return partitions, nil
}

// LatestOffset returns the next offset for the selected topic partition.
func (broker *Broker) LatestOffset(topic string, partition int) (int64, error) {
	if broker == nil {
		return 0, errors.New("get broker latest offset: nil broker")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.DataDir == "" {
		return 0, errors.New("get broker latest offset: empty data directory")
	}
	if topic == "" || strings.ContainsAny(topic, `/\`) {
		return 0, fmt.Errorf("get broker latest offset: invalid topic %q", topic)
	}
	if partition < 0 {
		return 0, fmt.Errorf("get broker latest offset: negative partition %d", partition)
	}

	identity := partitionKey{topic: topic, partition: partition}
	log := broker.logs[identity]
	if log == nil {
		path := filepath.Join(broker.DataDir, fmt.Sprintf("%s__%d.log", topic, partition))
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return 0, nil
		} else if err != nil {
			return 0, fmt.Errorf("find topic %q partition %d: %w", topic, partition, err)
		}
		openedLog, err := openBrokerPartitionLog(path)
		if err != nil {
			return 0, fmt.Errorf("open topic %q partition %d: %w", topic, partition, err)
		}
		if broker.logs == nil {
			broker.logs = make(map[partitionKey]*storage.PartitionLog)
		}
		log = openedLog
		broker.logs[identity] = log
	}

	offset, err := storage.LatestOffset(log)
	if err != nil {
		return 0, fmt.Errorf("get latest offset for topic %q partition %d: %w", topic, partition, err)
	}
	return offset, nil
}

// Close releases every partition log owned by the broker and joins close errors.
func (broker *Broker) Close() error {
	if broker == nil {
		return nil
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()

	var closeErrors []error
	for identity, log := range broker.logs {
		if err := storage.ClosePartitionLog(log); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close topic %q partition %d: %w", identity.topic, identity.partition, err))
		}
		delete(broker.logs, identity)
	}
	for identity, log := range broker.kafkaLogs {
		if err := closeBrokerKafkaPartitionLog(log); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close Kafka topic %q partition %d: %w", identity.topic, identity.partition, err))
		}
		delete(broker.kafkaLogs, identity)
	}
	return errors.Join(closeErrors...)
}
