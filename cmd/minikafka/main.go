package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"

	httpadapter "github.com/Rwatana/miniKafka/internal/adapter/http"
	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/infrastructure/postgres"
)

var prepareDatabase = postgres.PrepareDatabase
var listenAndServe = (*http.Server).ListenAndServe
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

	database, err := prepareDatabase(context.Background(), databaseURL)
	if err != nil {
		log.Printf("prepare broker database: %v", err)
		exitProcess(1)
		return
	}
	defer database.Close()

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
	log.Printf("MiniKafka broker listening on port %s", port)
	if err := listenAndServe(server); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("serve MiniKafka broker: %v", err)
		_ = brokerInstance.Close()
		_ = database.Close()
		exitProcess(1)
	}
}
