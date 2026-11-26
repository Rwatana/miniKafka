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
var serveKafka = kafkaprotocol.ServeAPIVersionsV3
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

	database, err := prepareDatabase(context.Background(), databaseURL)
	if err != nil {
		log.Printf("prepare broker database: %v", err)
		exitProcess(1)
		return
	}
	defer database.Close()

	kafkaListener, err := listenKafka("tcp", kafkaAddress)
	if err != nil {
		log.Printf("listen for Kafka clients on %s: %v", kafkaAddress, err)
		exitProcess(1)
		return
	}
	defer kafkaListener.Close()
	go func() {
		if err := serveKafka(kafkaListener, 100*1024*1024, []kafkaprotocol.APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}); err != nil {
			log.Printf("serve Kafka clients: %v", err)
		}
	}()

	brokerInstance := &broker.Broker{DataDir: dataDir}
	defer brokerInstance.Close()
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
}
