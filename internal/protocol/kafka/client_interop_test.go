package kafka

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/storage"
	kafkago "github.com/segmentio/kafka-go"
)

func TestKafkaGoClientProducesAndFetchesThroughKafka(t *testing.T) {
	dataDir := t.TempDir()
	emptyLog, err := storage.OpenKafkaPartitionLog(filepath.Join(dataDir, "orders__0.kafka.log"))
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
	}
	if err := storage.CloseKafkaPartitionLog(emptyLog); err != nil {
		t.Fatalf("CloseKafkaPartitionLog() error = %v", err)
	}
	brokerInstance := &broker.Broker{DataDir: dataDir}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	port := int32(listener.Addr().(*net.TCPAddr).Port)
	metadataProvider := func() ([]MetadataBroker, []MetadataTopic, error) {
		return []MetadataBroker{{NodeID: 0, Host: "127.0.0.1", Port: port}}, []MetadataTopic{{
			Name:       "orders",
			Partitions: []MetadataPartition{{PartitionIndex: 0, LeaderID: 0, ReplicaNodes: []int32{0}, ISRNodes: []int32{0}}},
		}}, nil
	}
	supported := []APIVersionRange{
		{APIKey: 0, MinVersion: 3, MaxVersion: 3},
		{APIKey: 1, MinVersion: 4, MaxVersion: 4},
		{APIKey: 2, MinVersion: 1, MaxVersion: 1},
		{APIKey: 3, MinVersion: 0, MaxVersion: 1},
		{APIKey: 18, MinVersion: 0, MaxVersion: 3},
	}
	serverDone := make(chan error, 1)
	go func() {
		readBatches := func(topic string, partition int, offset int64, maxBytes int32) ([]byte, int64, error) {
			return brokerInstance.ReadKafkaRecordBatches(topic, partition, offset, maxBytes)
		}
		lookupOffsets := func(topic string, partition int, timestamp int64) (int64, int64, error) {
			return brokerInstance.LookupKafkaOffset(topic, partition, timestamp)
		}
		serverDone <- ServeKafkaWithFetchReader(listener, 1024*1024, supported, metadataProvider, brokerInstance.AppendKafkaRecordBatch, readBatches, lookupOffsets)
	}()
	defer func() {
		_ = listener.Close()
		<-serverDone
		_ = brokerInstance.Close()
	}()

	address := listener.Addr().String()
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(address),
		Topic:                  "orders",
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchSize:              1,
		BatchTimeout:           time.Millisecond,
		AllowAutoTopicCreation: false,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wantKey := []byte("customer-1")
	wantValue := []byte("created")
	if err := writer.WriteMessages(ctx, kafkago.Message{Key: wantKey, Value: wantValue}); err != nil {
		t.Fatalf("Kafka client WriteMessages() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Kafka client writer Close() error = %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{address},
		Topic:     "orders",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1024,
	})
	defer reader.Close()
	if err := reader.SetOffset(0); err != nil {
		t.Fatalf("Kafka client SetOffset() error = %v", err)
	}
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("Kafka client ReadMessage() error = %v", err)
	}
	if string(message.Key) != string(wantKey) || string(message.Value) != string(wantValue) {
		t.Errorf("Kafka client message = (%q, %q), want (%q, %q)", message.Key, message.Value, wantKey, wantValue)
	}
}
