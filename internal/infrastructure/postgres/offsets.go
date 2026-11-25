// Package postgres stores MiniKafka consumer offsets in PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type offsetExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// CommitOffset atomically inserts or advances a group's offset for one partition.
func CommitOffset(ctx context.Context, executor offsetExecutor, group, topic string, partition int, offset int64) error {
	if ctx == nil {
		return errors.New("commit consumer offset: nil context")
	}
	if executor == nil {
		return errors.New("commit consumer offset: nil database executor")
	}
	if group == "" {
		return errors.New("commit consumer offset: empty consumer group")
	}
	if topic == "" {
		return errors.New("commit consumer offset: empty topic")
	}
	if partition < 0 {
		return fmt.Errorf("commit consumer offset: negative partition %d", partition)
	}
	if offset < 0 {
		return fmt.Errorf("commit consumer offset: negative offset %d", offset)
	}

	const query = `INSERT INTO consumer_offsets
		(consumer_group, topic, partition, committed_offset, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (consumer_group, topic, partition)
		DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
			updated_at = EXCLUDED.updated_at`
	if _, err := executor.ExecContext(ctx, query, group, topic, partition, offset); err != nil {
		return fmt.Errorf("commit offset for group %q topic %q partition %d: %w", group, topic, partition, err)
	}
	return nil
}
