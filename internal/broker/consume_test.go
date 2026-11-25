package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBrokerConsume(t *testing.T) {
	t.Run("reads cached records from an inclusive offset", func(t *testing.T) {
		broker := &Broker{DataDir: t.TempDir()}
		for _, value := range []string{"one", "two", "three"} {
			if _, err := broker.Produce("events", 0, nil, value); err != nil {
				t.Fatal(err)
			}
		}
		records, err := broker.Consume("events", 0, 1, 1)
		if err != nil {
			t.Fatalf("Consume() error = %v", err)
		}
		if len(records) != 1 || records[0].Offset != 1 || records[0].Value != "two" {
			t.Errorf("Consume() = %+v, want offset 1 value two", records)
		}
		closeBrokerLogs(t, broker)
	})

	t.Run("reopens an existing partition after restart", func(t *testing.T) {
		dataDir := t.TempDir()
		firstBroker := &Broker{DataDir: dataDir}
		if _, err := firstBroker.Produce("events", 2, nil, "persisted"); err != nil {
			t.Fatal(err)
		}
		closeBrokerLogs(t, firstBroker)

		restarted := &Broker{DataDir: dataDir}
		records, err := restarted.Consume("events", 2, 0, 10)
		if err != nil {
			t.Fatalf("Consume() after restart error = %v", err)
		}
		if len(records) != 1 || records[0].Value != "persisted" {
			t.Errorf("Consume() after restart = %+v, want persisted record", records)
		}
		if len(restarted.logs) != 1 {
			t.Errorf("cached logs = %d, want 1", len(restarted.logs))
		}
		closeBrokerLogs(t, restarted)
	})

	t.Run("absent partition is empty and does not create a file", func(t *testing.T) {
		dataDir := t.TempDir()
		broker := &Broker{DataDir: dataDir}
		records, err := broker.Consume("missing", 0, 0, 10)
		if err != nil || len(records) != 0 {
			t.Errorf("Consume() = (%v, %v), want empty", records, err)
		}
		if _, err := os.Stat(filepath.Join(dataDir, "missing__0.log")); !os.IsNotExist(err) {
			t.Errorf("missing partition log stat error = %v, want not exist", err)
		}
	})

	t.Run("returns empty for zero maximum", func(t *testing.T) {
		records, err := (&Broker{DataDir: t.TempDir()}).Consume("events", 0, 0, 0)
		if err != nil || len(records) != 0 {
			t.Errorf("Consume() = (%v, %v), want empty", records, err)
		}
	})

	t.Run("rejects invalid state and arguments", func(t *testing.T) {
		if _, err := (*Broker)(nil).Consume("events", 0, 0, 1); err == nil {
			t.Error("nil broker error = nil, want error")
		}
		broker := &Broker{}
		if _, err := broker.Consume("events", 0, 0, 1); err == nil {
			t.Error("empty data directory error = nil, want error")
		}
		broker.DataDir = t.TempDir()
		for _, input := range []struct {
			topic     string
			partition int
			offset    int64
			max       int
		}{
			{topic: "../escape", partition: 0, offset: 0, max: 1},
			{topic: "events", partition: -1, offset: 0, max: 1},
			{topic: "events", partition: 0, offset: -1, max: 1},
			{topic: "events", partition: 0, offset: 0, max: -1},
		} {
			if _, err := broker.Consume(input.topic, input.partition, input.offset, input.max); err == nil {
				t.Errorf("Consume(%+v) error = nil, want error", input)
			}
		}
	})

	t.Run("reports filesystem errors", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "file-not-directory")
		if err := os.WriteFile(dataDir, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: dataDir}).Consume("events", 0, 0, 1); err == nil {
			t.Error("Consume() error = nil, want filesystem error")
		}
	})

	t.Run("reports partition open and record decode errors", func(t *testing.T) {
		openDataDir := t.TempDir()
		if err := os.Mkdir(filepath.Join(openDataDir, "events__0.log"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: openDataDir}).Consume("events", 0, 0, 1); err == nil {
			t.Error("Consume() error = nil, want partition open error")
		}

		decodeDataDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(decodeDataDir, "events__0.log"), []byte("{invalid}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (&Broker{DataDir: decodeDataDir}).Consume("events", 0, 0, 1); err == nil {
			t.Error("Consume() error = nil, want record decode error")
		}
	})
}
