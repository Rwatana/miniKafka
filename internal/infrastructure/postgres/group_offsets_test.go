package postgres

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
)

func TestListOffsetsForGroup(t *testing.T) {
	t.Run("lists positions in topic partition order", func(t *testing.T) {
		database := openOffsetQueryDB(t, "group-list")
		got, err := ListOffsetsForGroup(context.Background(), database, "workers")
		if err != nil {
			t.Fatalf("ListOffsetsForGroup() error = %v", err)
		}
		want := []GroupOffset{{Topic: "events", Partition: 0, CommittedOffset: 7}, {Topic: "orders", Partition: 2, CommittedOffset: 21}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ListOffsetsForGroup() = %+v, want %+v", got, want)
		}
		if !strings.Contains(lastOffsetQuery, "WHERE consumer_group = $1 ORDER BY topic, partition") {
			t.Errorf("query = %q, want group filter and stable ordering", lastOffsetQuery)
		}
		wantArgs := []driver.NamedValue{{Ordinal: 1, Value: "workers"}}
		if !reflect.DeepEqual(lastOffsetArgs, wantArgs) {
			t.Errorf("query args = %#v, want %#v", lastOffsetArgs, wantArgs)
		}
	})

	t.Run("returns an empty list when the group has no commits", func(t *testing.T) {
		database := openOffsetQueryDB(t, "empty-list")
		got, err := ListOffsetsForGroup(context.Background(), database, "new-group")
		if err != nil || got == nil || len(got) != 0 {
			t.Errorf("ListOffsetsForGroup() = (%+v, %v), want empty list", got, err)
		}
	})

	t.Run("rejects invalid input", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			ctx   context.Context
			db    offsetRowsQueryer
			group string
		}{
			{name: "nil context", db: openOffsetQueryDB(t, "group-list"), group: "workers"},
			{name: "nil queryer", ctx: context.Background(), group: "workers"},
			{name: "empty group", ctx: context.Background(), db: openOffsetQueryDB(t, "group-list")},
		} {
			t.Run(test.name, func(t *testing.T) {
				if _, err := ListOffsetsForGroup(test.ctx, test.db, test.group); err == nil {
					t.Error("ListOffsetsForGroup() error = nil, want validation error")
				}
			})
		}
	})

	t.Run("wraps query errors", func(t *testing.T) {
		database := openOffsetQueryDB(t, "error")
		if _, err := ListOffsetsForGroup(context.Background(), database, "workers"); err == nil || !strings.Contains(err.Error(), "query failed") {
			t.Errorf("ListOffsetsForGroup() error = %v, want query error", err)
		}
	})

	t.Run("wraps scan errors", func(t *testing.T) {
		database := openOffsetQueryDB(t, "scan-error")
		if _, err := ListOffsetsForGroup(context.Background(), database, "workers"); err == nil {
			t.Error("ListOffsetsForGroup() error = nil, want scan error")
		}
	})

	t.Run("wraps iteration errors", func(t *testing.T) {
		database := openOffsetQueryDB(t, "rows-error")
		if _, err := ListOffsetsForGroup(context.Background(), database, "workers"); err == nil || !strings.Contains(err.Error(), "iteration failed") {
			t.Errorf("ListOffsetsForGroup() error = %v, want iteration error", err)
		}
	})
}
