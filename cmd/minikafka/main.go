package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"

	httpadapter "github.com/Rwatana/miniKafka/internal/adapter/http"
	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/infrastructure/postgres"
	kafkaprotocol "github.com/Rwatana/miniKafka/internal/protocol/kafka"
)

var prepareDatabase = postgres.PrepareDatabase
var listenAndServe = (*http.Server).ListenAndServe
var listenKafka = net.Listen
var serveKafka = kafkaprotocol.ServeKafkaWithFetchReader
var exitProcess = os.Exit

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/minikafka"
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "5000"
	}
	kafkaAddress := os.Getenv("KAFKA_LISTEN_ADDR")
	if kafkaAddress == "" {
		kafkaAddress = ":9092"
	}
	advertisedHost := os.Getenv("KAFKA_ADVERTISED_HOST")
	if advertisedHost == "" {
		advertisedHost = "localhost"
	}

	database, err := prepareDatabase(context.Background(), databaseURL)
	if err != nil {
		log.Printf("prepare broker database: %v", err)
		exitProcess(1)
		return
	}
	defer database.Close()

	brokerInstance := &broker.Broker{DataDir: dataDir}
	defer brokerInstance.Close()

	kafkaListener, err := listenKafka("tcp", kafkaAddress)
	if err != nil {
		log.Printf("listen for Kafka clients on %s: %v", kafkaAddress, err)
		exitProcess(1)
		return
	}
	defer kafkaListener.Close()
	advertisedPort := int32(kafkaListener.Addr().(*net.TCPAddr).Port)
	metadataProvider := func() ([]kafkaprotocol.MetadataBroker, []kafkaprotocol.MetadataTopic, error) {
		partitions, err := brokerInstance.ListPartitions()
		if err != nil {
			return nil, nil, err
		}
		brokers := []kafkaprotocol.MetadataBroker{{NodeID: 0, Host: advertisedHost, Port: advertisedPort}}
		return brokers, metadataTopicsFromPartitions(partitions, 0), nil
	}
	kafkaDone := make(chan struct{})
	go func() {
		defer close(kafkaDone)
		supportedAPIs := []kafkaprotocol.APIVersionRange{
			{APIKey: 0, MinVersion: 3, MaxVersion: 3},
			{APIKey: 1, MinVersion: 4, MaxVersion: 4},
			{APIKey: 2, MinVersion: 1, MaxVersion: 1},
			{APIKey: 3, MinVersion: 0, MaxVersion: 1},
			{APIKey: 18, MinVersion: 0, MaxVersion: 3},
		}
		if err := serveKafka(kafkaListener, 100*1024*1024, supportedAPIs, metadataProvider, brokerInstance.AppendKafkaRecordBatch, brokerInstance.ReadKafkaRecordBatches, brokerInstance.LookupKafkaOffset); err != nil {
			log.Printf("serve Kafka clients: %v", err)
		}
	}()

	getCommittedOffset := func(ctx context.Context, group, topic string, partition int) (int64, error) {
		return postgres.GetCommittedOffset(ctx, database, group, topic, partition)
	}
	commitOffset := func(ctx context.Context, group, topic string, partition int, offset int64) error {
		return postgres.CommitOffset(ctx, database, group, topic, partition, offset)
	}
	listOffsetsForGroup := func(ctx context.Context, group string) ([]postgres.GroupOffset, error) {
		return postgres.ListOffsetsForGroup(ctx, database, group)
	}

	router := httpadapter.NewRouter(
		httpadapter.ProduceHandler{Broker: brokerInstance},
		httpadapter.ConsumeHandler{Broker: brokerInstance, GetCommittedOffset: getCommittedOffset},
		httpadapter.CommitHandler{CommitOffset: commitOffset},
		httpadapter.TopicsHandler{Broker: brokerInstance},
		httpadapter.LagHandler{ListOffsetsForGroup: listOffsetsForGroup, LatestOffset: brokerInstance.LatestOffset},
		httpadapter.DashboardHandler{},
	)
	server := &http.Server{Addr: ":" + port, Handler: router}
	log.Printf("MiniKafka HTTP API listening on port %s and Kafka protocol on %s", port, kafkaAddress)
	if err := listenAndServe(server); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("serve MiniKafka broker: %v", err)
		_ = brokerInstance.Close()
		_ = database.Close()
		exitProcess(1)
	}
	_ = kafkaListener.Close()
	<-kafkaDone
}
