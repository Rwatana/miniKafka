package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

var registerOffsetQueryDriver sync.Once
var lastOffsetQuery string
var lastOffsetArgs []driver.NamedValue

type offsetQueryDriver struct{}

func (offsetQueryDriver) Open(name string) (driver.Conn, error) {
	return offsetQueryConn{mode: name}, nil
}

type offsetQueryConn struct{ mode string }

func (offsetQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}
func (offsetQueryConn) Close() error { return nil }
func (offsetQueryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (connection offsetQueryConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	lastOffsetQuery = query
	lastOffsetArgs = append([]driver.NamedValue(nil), args...)
	if connection.mode == "error" {
		return nil, errors.New("query failed")
	}
	rows := &offsetQueryRows{}
	if connection.mode == "stored" {
		rows.values = []driver.Value{int64(23)}
	}
	return rows, nil
}

type offsetQueryRows struct {
	values []driver.Value
	read   bool
}

func (rows *offsetQueryRows) Columns() []string { return []string{"committed_offset"} }
func (rows *offsetQueryRows) Close() error      { return nil }
func (rows *offsetQueryRows) Next(destination []driver.Value) error {
	if rows.read || rows.values == nil {
		return io.EOF
	}
	rows.read = true
	copy(destination, rows.values)
	return nil
}

func openOffsetQueryDB(t *testing.T, mode string) *sql.DB {
	t.Helper()
	registerOffsetQueryDriver.Do(func() { sql.Register("minikafka-offset-query-test", offsetQueryDriver{}) })
	database, err := sql.Open("minikafka-offset-query-test", mode)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = database.Close() })
	lastOffsetQuery = ""
	lastOffsetArgs = nil
	return database
}

func TestGetCommittedOffset(t *testing.T) {
	t.Run("loads by the complete partition key", func(t *testing.T) {
		database := openOffsetQueryDB(t, "stored")
		offset, err := GetCommittedOffset(context.Background(), database, "orders-workers", "orders", 2)
		if err != nil || offset != 23 {
			t.Fatalf("GetCommittedOffset() = (%d, %v), want (23, nil)", offset, err)
		}
		if !strings.Contains(lastOffsetQuery, "consumer_group = $1 AND topic = $2 AND partition = $3") {
			t.Errorf("query = %q, want complete consumer group/topic/partition key", lastOffsetQuery)
		}
		wantArgs := []driver.NamedValue{{Ordinal: 1, Value: "orders-workers"}, {Ordinal: 2, Value: "orders"}, {Ordinal: 3, Value: int64(2)}}
		if !reflect.DeepEqual(lastOffsetArgs, wantArgs) {
			t.Errorf("query args = %#v, want %#v", lastOffsetArgs, wantArgs)
		}
	})

	t.Run("starts new groups at zero", func(t *testing.T) {
		database := openOffsetQueryDB(t, "missing")
		offset, err := GetCommittedOffset(context.Background(), database, "new-group", "orders", 0)
		if err != nil || offset != 0 {
			t.Errorf("GetCommittedOffset() = (%d, %v), want (0, nil)", offset, err)
		}
	})

	t.Run("rejects invalid input without querying", func(t *testing.T) {
		cases := []struct {
			name      string
			ctx       context.Context
			queryer   offsetQueryer
			group     string
			topic     string
			partition int
		}{
			{name: "nil context", queryer: openOffsetQueryDB(t, "stored"), group: "group", topic: "topic"},
			{name: "nil queryer", ctx: context.Background(), group: "group", topic: "topic"},
			{name: "empty group", ctx: context.Background(), queryer: openOffsetQueryDB(t, "stored"), topic: "topic"},
			{name: "empty topic", ctx: context.Background(), queryer: openOffsetQueryDB(t, "stored"), group: "group"},
			{name: "negative partition", ctx: context.Background(), queryer: openOffsetQueryDB(t, "stored"), group: "group", topic: "topic", partition: -1},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				if offset, err := GetCommittedOffset(test.ctx, test.queryer, test.group, test.topic, test.partition); err == nil || offset != 0 {
					t.Errorf("GetCommittedOffset() = (%d, %v), want validation error", offset, err)
				}
			})
		}
	})

	t.Run("wraps query errors", func(t *testing.T) {
		database := openOffsetQueryDB(t, "error")
		if _, err := GetCommittedOffset(context.Background(), database, "group", "topic", 0); err == nil || !strings.Contains(err.Error(), "query failed") {
			t.Errorf("GetCommittedOffset() error = %v, want wrapped query error", err)
		}
	})
}
