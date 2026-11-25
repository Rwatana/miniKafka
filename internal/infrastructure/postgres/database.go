package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var consumerOffsetSchema string

var openPostgresDatabase = sql.Open

// PrepareDatabase opens PostgreSQL, verifies connectivity, and applies the consumer offset schema.
func PrepareDatabase(ctx context.Context, dataSourceName string) (*sql.DB, error) {
	if ctx == nil {
		return nil, errors.New("prepare postgres database: nil context")
	}
	if strings.TrimSpace(dataSourceName) == "" {
		return nil, errors.New("prepare postgres database: empty data source name")
	}

	database, err := openPostgresDatabase("pgx", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open postgres database: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		closeErr := database.Close()
		return nil, errors.Join(fmt.Errorf("ping postgres database: %w", err), closeErr)
	}
	if _, err := database.ExecContext(ctx, consumerOffsetSchema); err != nil {
		closeErr := database.Close()
		return nil, errors.Join(fmt.Errorf("apply consumer offset schema: %w", err), closeErr)
	}
	return database, nil
}
