package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestPrepareDatabase(t *testing.T) {
	t.Run("pings and applies embedded schema", func(t *testing.T) {
		originalOpen := openPostgresDatabase
		var database *sql.DB
		openPostgresDatabase = func(driverName, dataSourceName string) (*sql.DB, error) {
			if driverName != "pgx" {
				t.Errorf("driver name = %q, want pgx", driverName)
			}
			if dataSourceName != "ready" {
				t.Errorf("data source name = %q, want ready", dataSourceName)
			}
			database = openOffsetQueryDB(t, dataSourceName)
			return database, nil
		}
		defer func() { openPostgresDatabase = originalOpen }()

		prepared, err := PrepareDatabase(context.Background(), "ready")
		if err != nil {
			t.Fatalf("PrepareDatabase() error = %v", err)
		}
		if prepared != database {
			t.Error("PrepareDatabase() returned a different database handle")
		}
		if !strings.Contains(lastOffsetExec, "CREATE TABLE IF NOT EXISTS consumer_offsets") {
			t.Errorf("schema statement = %q, want embedded consumer_offsets schema", lastOffsetExec)
		}
		if err := prepared.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("rejects nil context and empty data source", func(t *testing.T) {
		originalOpen := openPostgresDatabase
		openCalls := 0
		openPostgresDatabase = func(string, string) (*sql.DB, error) {
			openCalls++
			return nil, nil
		}
		defer func() { openPostgresDatabase = originalOpen }()
		if _, err := PrepareDatabase(nil, "dsn"); err == nil {
			t.Error("PrepareDatabase(nil) error = nil, want validation error")
		}
		if _, err := PrepareDatabase(context.Background(), "  "); err == nil {
			t.Error("PrepareDatabase(empty DSN) error = nil, want validation error")
		}
		if openCalls != 0 {
			t.Errorf("database open calls = %d, want 0", openCalls)
		}
	})

	t.Run("wraps open errors", func(t *testing.T) {
		originalOpen := openPostgresDatabase
		openPostgresDatabase = func(string, string) (*sql.DB, error) { return nil, errors.New("open failed") }
		defer func() { openPostgresDatabase = originalOpen }()
		if _, err := PrepareDatabase(context.Background(), "dsn"); err == nil || !strings.Contains(err.Error(), "open failed") {
			t.Errorf("PrepareDatabase() error = %v, want open error", err)
		}
	})

	t.Run("closes database after ping errors", func(t *testing.T) {
		originalOpen := openPostgresDatabase
		var database *sql.DB
		openPostgresDatabase = func(string, string) (*sql.DB, error) {
			database = openOffsetQueryDB(t, "ping-error")
			return database, nil
		}
		defer func() { openPostgresDatabase = originalOpen }()
		if _, err := PrepareDatabase(context.Background(), "dsn"); err == nil || !strings.Contains(err.Error(), "ping failed") {
			t.Errorf("PrepareDatabase() error = %v, want ping error", err)
		}
		if err := database.Ping(); err == nil || !strings.Contains(err.Error(), "database is closed") {
			t.Errorf("Ping() after failure = %v, want closed database", err)
		}
	})

	t.Run("closes database after schema errors", func(t *testing.T) {
		originalOpen := openPostgresDatabase
		var database *sql.DB
		openPostgresDatabase = func(string, string) (*sql.DB, error) {
			database = openOffsetQueryDB(t, "exec-error")
			return database, nil
		}
		defer func() { openPostgresDatabase = originalOpen }()
		if _, err := PrepareDatabase(context.Background(), "dsn"); err == nil || !strings.Contains(err.Error(), "exec failed") {
			t.Errorf("PrepareDatabase() error = %v, want schema error", err)
		}
		if err := database.Ping(); err == nil || !strings.Contains(err.Error(), "database is closed") {
			t.Errorf("Ping() after failure = %v, want closed database", err)
		}
	})
}
