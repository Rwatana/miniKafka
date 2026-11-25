//go:build integration

package httpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Rwatana/miniKafka/internal/broker"
	"github.com/Rwatana/miniKafka/internal/infrastructure/postgres"
)

func TestBrokerFlowsWithPostgres(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx := context.Background()
	database, err := postgres.PrepareDatabase(ctx, dsn)
	if err != nil {
		t.Fatalf("PrepareDatabase() error = %v", err)
	}
	defer database.Close()
	group := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	store := &broker.Broker{DataDir: t.TempDir()}
	defer store.Close()

	router := NewRouter(
		ProduceHandler{Broker: store},
		ConsumeHandler{Broker: store, GetCommittedOffset: func(ctx context.Context, group, topic string, partition int) (int64, error) {
			return postgres.GetCommittedOffset(ctx, database, group, topic, partition)
		}},
		CommitHandler{CommitOffset: func(ctx context.Context, group, topic string, partition int, offset int64) error {
			return postgres.CommitOffset(ctx, database, group, topic, partition, offset)
		}},
		TopicsHandler{Broker: store},
		LagHandler{
			ListOffsetsForGroup: func(ctx context.Context, group string) ([]GroupOffset, error) {
				return postgres.ListOffsetsForGroup(ctx, database, group)
			},
			LatestOffset: store.LatestOffset,
		},
		DashboardHandler{},
	)

	produced := performProduce(router, `{"topic":"orders","key":"order-1","value":"created"}`)
	if produced.Code != http.StatusOK {
		t.Fatalf("POST /produce status = %d: %s", produced.Code, produced.Body)
	}

	consumed := performConsume(router, http.MethodGet, "/consume?topic=orders&group="+group)
	if consumed.Code != http.StatusOK {
		t.Fatalf("GET /consume status = %d: %s", consumed.Code, consumed.Body)
	}
	var firstBatch consumeResponse
	if err := json.NewDecoder(consumed.Body).Decode(&firstBatch); err != nil {
		t.Fatal(err)
	}
	if firstBatch.FromOffset != 0 || len(firstBatch.Messages) != 1 || firstBatch.Messages[0].Value != "created" {
		t.Fatalf("first consume response = %+v, want one record from offset 0", firstBatch)
	}

	committed := performCommit(router, http.MethodPost, fmt.Sprintf(`{"group":%q,"topic":"orders","offset":0}`, group))
	if committed.Code != http.StatusOK {
		t.Fatalf("initial POST /commit status = %d: %s", committed.Code, committed.Body)
	}
	committed = performCommit(router, http.MethodPost, fmt.Sprintf(`{"group":%q,"topic":"orders","offset":1}`, group))
	if committed.Code != http.StatusOK {
		t.Fatalf("updated POST /commit status = %d: %s", committed.Code, committed.Body)
	}
	consumed = performConsume(router, http.MethodGet, "/consume?topic=orders&group="+group)
	var secondBatch consumeResponse
	if err := json.NewDecoder(consumed.Body).Decode(&secondBatch); err != nil {
		t.Fatal(err)
	}
	if consumed.Code != http.StatusOK || secondBatch.FromOffset != 1 || len(secondBatch.Messages) != 0 {
		t.Errorf("second consume = %d %+v, want empty batch from committed offset 1", consumed.Code, secondBatch)
	}

	topics := performTopics(router, http.MethodGet)
	var topicRows []topicResponse
	if err := json.NewDecoder(topics.Body).Decode(&topicRows); err != nil {
		t.Fatal(err)
	}
	if topics.Code != http.StatusOK || len(topicRows) != 1 || topicRows[0].LatestOffset != 1 {
		t.Errorf("GET /topics = %d %+v, want orders/0 latest offset 1", topics.Code, topicRows)
	}

	lag := performLag(router, http.MethodGet, "/lag?group="+group)
	var lagRows []lagResponse
	if err := json.NewDecoder(lag.Body).Decode(&lagRows); err != nil {
		t.Fatal(err)
	}
	if lag.Code != http.StatusOK || len(lagRows) != 1 || lagRows[0].CommittedOffset != 1 || lagRows[0].LatestOffset != 1 || lagRows[0].Lag != 0 {
		t.Errorf("GET /lag = %d %+v, want committed/latest 1 and lag 0", lag.Code, lagRows)
	}
}
