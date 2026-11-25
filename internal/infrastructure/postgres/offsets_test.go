package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type offsetExecutorFunc func(context.Context, string, ...any) (sql.Result, error)

func (executor offsetExecutorFunc) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return executor(ctx, query, args...)
}

func TestCommitOffset(t *testing.T) {
	t.Run("upserts the selected partition", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), struct{}{}, "request")
		calls := 0
		executor := offsetExecutorFunc(func(gotContext context.Context, query string, args ...any) (sql.Result, error) {
			calls++
			if gotContext != ctx {
				t.Errorf("ExecContext context = %v, want original context", gotContext)
			}
			if !strings.Contains(query, "ON CONFLICT (consumer_group, topic, partition)") || !strings.Contains(query, "DO UPDATE SET committed_offset = EXCLUDED.committed_offset") {
				t.Errorf("ExecContext query = %q, want composite-key upsert", query)
			}
			want := []any{"orders-workers", "orders", 2, int64(17)}
			if len(args) != len(want) {
				t.Fatalf("ExecContext args = %v, want %v", args, want)
			}
			for index := range want {
				if args[index] != want[index] {
					t.Errorf("ExecContext arg %d = %v, want %v", index, args[index], want[index])
				}
			}
			return nil, nil
		})

		if err := CommitOffset(ctx, executor, "orders-workers", "orders", 2, 17); err != nil {
			t.Fatalf("CommitOffset() error = %v", err)
		}
		if calls != 1 {
			t.Errorf("ExecContext calls = %d, want 1", calls)
		}
	})

	t.Run("rejects invalid input without querying", func(t *testing.T) {
		cases := []struct {
			name      string
			ctx       context.Context
			executor  offsetExecutor
			group     string
			topic     string
			partition int
			offset    int64
		}{
			{name: "nil context", executor: offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
				t.Error("unexpected query")
				return nil, nil
			}), group: "group", topic: "topic"},
			{name: "nil executor", ctx: context.Background(), group: "group", topic: "topic"},
			{name: "empty group", ctx: context.Background(), executor: offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
				t.Error("unexpected query")
				return nil, nil
			}), topic: "topic"},
			{name: "empty topic", ctx: context.Background(), executor: offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
				t.Error("unexpected query")
				return nil, nil
			}), group: "group"},
			{name: "negative partition", ctx: context.Background(), executor: offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
				t.Error("unexpected query")
				return nil, nil
			}), group: "group", topic: "topic", partition: -1},
			{name: "negative offset", ctx: context.Background(), executor: offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
				t.Error("unexpected query")
				return nil, nil
			}), group: "group", topic: "topic", offset: -1},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				if err := CommitOffset(test.ctx, test.executor, test.group, test.topic, test.partition, test.offset); err == nil {
					t.Fatal("CommitOffset() error = nil, want validation error")
				}
			})
		}
	})

	t.Run("wraps database errors", func(t *testing.T) {
		databaseError := errors.New("database unavailable")
		executor := offsetExecutorFunc(func(context.Context, string, ...any) (sql.Result, error) {
			return nil, databaseError
		})
		if err := CommitOffset(context.Background(), executor, "group", "topic", 0, 3); !errors.Is(err, databaseError) {
			t.Errorf("CommitOffset() error = %v, want wrapped database error", err)
		}
	})
}
