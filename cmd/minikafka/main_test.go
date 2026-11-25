package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var registerAppTestDriver sync.Once

type appTestDriver struct{}

func (appTestDriver) Open(string) (driver.Conn, error) { return appTestConn{}, nil }

type appTestConn struct{}

func (appTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}
func (appTestConn) Close() error { return nil }
func (appTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}
func (appTestConn) Ping(context.Context) error { return errors.New("database unavailable") }
func (appTestConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("database unavailable")
}
func (appTestConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("database unavailable")
}

func TestMain(t *testing.T) {
	t.Run("uses defaults, wires routes, and handles a normal stop", func(t *testing.T) {
		workingDirectory, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		temporaryDirectory := t.TempDir()
		if err := os.Chdir(temporaryDirectory); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chdir(workingDirectory) }()
		t.Setenv("DATABASE_URL", "")
		t.Setenv("DATA_DIR", "")
		t.Setenv("PORT", "")

		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalExit := exitProcess
		exitCode := 0
		prepareDatabase = func(_ context.Context, dsn string) (*sql.DB, error) {
			if dsn != "postgres://postgres:postgres@localhost:5432/minikafka" {
				t.Errorf("database URL = %q, want reference default", dsn)
			}
			registerAppTestDriver.Do(func() { sql.Register("minikafka-app-test", appTestDriver{}) })
			database, err := sql.Open("minikafka-app-test", "")
			if err != nil {
				return nil, err
			}
			database.SetMaxOpenConns(1)
			return database, nil
		}
		listenAndServe = func(server *http.Server) error {
			if server.Addr != ":5000" {
				t.Errorf("server address = %q, want default :5000", server.Addr)
			}
			for _, request := range []struct {
				method string
				path   string
				body   string
				want   int
			}{
				{method: http.MethodPost, path: "/produce", body: `{"topic":"orders","value":"created"}`, want: http.StatusOK},
				{method: http.MethodGet, path: "/consume?topic=orders&group=workers", want: http.StatusInternalServerError},
				{method: http.MethodPost, path: "/commit", body: `{"group":"workers","topic":"orders","offset":1}`, want: http.StatusInternalServerError},
				{method: http.MethodGet, path: "/topics", want: http.StatusOK},
				{method: http.MethodGet, path: "/lag?group=workers", want: http.StatusInternalServerError},
				{method: http.MethodGet, path: "/", want: http.StatusOK},
			} {
				recorder := httptest.NewRecorder()
				httpRequest := httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
				server.Handler.ServeHTTP(recorder, httpRequest)
				if recorder.Code != request.want {
					t.Errorf("%s %s status = %d, want %d: %s", request.method, request.path, recorder.Code, request.want, recorder.Body)
				}
			}
			return nil
		}
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			exitProcess = originalExit
		}()

		main()
		if exitCode != 0 {
			t.Errorf("exit code = %d, want 0", exitCode)
		}
		if _, err := os.Stat(filepath.Join(temporaryDirectory, "data", "orders__0.log")); err != nil {
			t.Errorf("produced partition log stat error = %v", err)
		}
	})

	t.Run("uses supplied environment and reports server failures", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example/db")
		t.Setenv("DATA_DIR", t.TempDir())
		t.Setenv("PORT", "8123")
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalExit := exitProcess
		exitCode := 0
		prepareDatabase = func(_ context.Context, dsn string) (*sql.DB, error) {
			if dsn != "postgres://example/db" {
				t.Errorf("database URL = %q, want supplied URL", dsn)
			}
			return sql.Open("minikafka-app-test", "")
		}
		listenAndServe = func(server *http.Server) error {
			if server.Addr != ":8123" {
				t.Errorf("server address = %q, want :8123", server.Addr)
			}
			return errors.New("listen failed")
		}
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			exitProcess = originalExit
		}()

		main()
		if exitCode != 1 {
			t.Errorf("exit code = %d, want 1", exitCode)
		}
	})

	t.Run("does not treat a normal server shutdown as failure", func(t *testing.T) {
		t.Setenv("DATA_DIR", t.TempDir())
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return sql.Open("minikafka-app-test", "") }
		listenAndServe = func(*http.Server) error { return http.ErrServerClosed }
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			exitProcess = originalExit
		}()

		main()
		if exitCode != 0 {
			t.Errorf("exit code = %d, want 0", exitCode)
		}
	})

	t.Run("reports database preparation failures", func(t *testing.T) {
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return nil, fmt.Errorf("database unavailable") }
		listenCalled := false
		listenAndServe = func(*http.Server) error { listenCalled = true; return nil }
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			exitProcess = originalExit
		}()

		main()
		if exitCode != 1 || listenCalled {
			t.Errorf("exit code = %d, listen called = %v, want failure before listening", exitCode, listenCalled)
		}
	})
}
