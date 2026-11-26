package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Rwatana/miniKafka/internal/kafkaerr"
	kafkaprotocol "github.com/Rwatana/miniKafka/internal/protocol/kafka"
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
		t.Setenv("KAFKA_LISTEN_ADDR", "")
		t.Setenv("KAFKA_ADVERTISED_HOST", "")

		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		exitCode := 0
		var kafkaListener *appTestListener
		serveKafkaDone := make(chan struct{})
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
		listenKafka = func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != ":9092" {
				t.Errorf("Kafka listener = %q %q, want tcp :9092", network, address)
			}
			kafkaListener = &appTestListener{}
			return kafkaListener, nil
		}
		serveKafka = func(_ net.Listener, maxFrameBytes int32, supported []kafkaprotocol.APIVersionRange, metadataProvider kafkaprotocol.MetadataProvider, appendBatch kafkaprotocol.KafkaBatchAppender, readBatches kafkaprotocol.KafkaBatchReader, offsetLookups ...kafkaprotocol.KafkaOffsetLookup) error {
			defer close(serveKafkaDone)
			if maxFrameBytes != 100*1024*1024 {
				t.Errorf("Kafka max frame size = %d, want 100 MiB", maxFrameBytes)
			}
			wantSupported := []kafkaprotocol.APIVersionRange{{APIKey: 0, MinVersion: 3, MaxVersion: 3}, {APIKey: 1, MinVersion: 2, MaxVersion: 4}, {APIKey: 2, MinVersion: 1, MaxVersion: 1}, {APIKey: 3, MinVersion: 0, MaxVersion: 1}, {APIKey: 18, MinVersion: 0, MaxVersion: 3}}
			if !reflect.DeepEqual(supported, wantSupported) {
				t.Errorf("supported Kafka APIs = %+v, want %+v", supported, wantSupported)
			}
			if metadataProvider == nil {
				t.Error("metadata provider = nil, want live broker metadata")
			} else {
				brokers, _, err := metadataProvider()
				if err != nil {
					t.Errorf("metadataProvider() error = %v", err)
				}
				wantBroker := []kafkaprotocol.MetadataBroker{{NodeID: 0, Host: "localhost", Port: 9092}}
				if !reflect.DeepEqual(brokers, wantBroker) {
					t.Errorf("metadata brokers = %+v, want %+v", brokers, wantBroker)
				}
			}
			if appendBatch == nil {
				t.Error("Produce appender = nil, want broker callback")
			}
			if readBatches == nil {
				t.Error("Fetch reader = nil, want broker callback")
			} else if records, highWatermark, err := readBatches("missing", 0, 0, 1024); !errors.Is(err, kafkaerr.ErrUnknownTopicOrPartition) || records != nil || highWatermark != 0 {
				t.Errorf("missing Fetch read = (%x, %d, %v), want (nil, 0, unknown topic/partition)", records, highWatermark, err)
			}
			if len(offsetLookups) != 1 || offsetLookups[0] == nil {
				t.Errorf("Kafka offset lookups = %d, want one broker callback", len(offsetLookups))
			} else if _, _, err := offsetLookups[0]("missing", 0, -2); !errors.Is(err, kafkaerr.ErrUnknownTopicOrPartition) {
				t.Errorf("missing Kafka offset lookup error = %v, want unknown topic/partition", err)
			}
			return nil
		}
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
			exitProcess = originalExit
		}()

		main()
		select {
		case <-serveKafkaDone:
		default:
			t.Error("main returned before Kafka listener stopped")
		}
		if exitCode != 0 {
			t.Errorf("exit code = %d, want 0", exitCode)
		}
		if kafkaListener == nil || !kafkaListener.closed {
			t.Error("Kafka TCP listener was not closed on shutdown")
		}
		if _, err := os.Stat(filepath.Join(temporaryDirectory, "data", "orders__0.log")); err != nil {
			t.Errorf("produced partition log stat error = %v", err)
		}
	})

	t.Run("uses supplied environment and reports server failures", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example/db")
		t.Setenv("DATA_DIR", t.TempDir())
		t.Setenv("PORT", "8123")
		t.Setenv("KAFKA_LISTEN_ADDR", "127.0.0.1:9093")
		t.Setenv("KAFKA_ADVERTISED_HOST", "broker.internal")
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		originalLogOutput := log.Writer()
		exitCode := 0
		logMessages := make(chan string, 8)
		log.SetOutput(appLogWriter{messages: logMessages})
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
			for {
				if strings.Contains(<-logMessages, "serve Kafka clients: protocol failed") {
					break
				}
			}
			return errors.New("listen failed")
		}
		listenKafka = func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != "127.0.0.1:9093" {
				t.Errorf("Kafka listener = %q %q, want tcp 127.0.0.1:9093", network, address)
			}
			return &appTestListener{}, nil
		}
		serveKafka = func(_ net.Listener, _ int32, _ []kafkaprotocol.APIVersionRange, metadataProvider kafkaprotocol.MetadataProvider, _ kafkaprotocol.KafkaBatchAppender, _ kafkaprotocol.KafkaBatchReader, _ ...kafkaprotocol.KafkaOffsetLookup) error {
			brokers, topics, err := metadataProvider()
			if err != nil {
				t.Errorf("metadataProvider() error = %v", err)
			}
			wantBrokers := []kafkaprotocol.MetadataBroker{{NodeID: 0, Host: "broker.internal", Port: 9092}}
			if !reflect.DeepEqual(brokers, wantBrokers) || len(topics) != 0 {
				t.Errorf("metadata snapshot = (%+v, %+v), want (%+v, empty)", brokers, topics, wantBrokers)
			}
			return errors.New("protocol failed")
		}
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
			exitProcess = originalExit
			log.SetOutput(originalLogOutput)
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
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return sql.Open("minikafka-app-test", "") }
		listenAndServe = func(*http.Server) error { return http.ErrServerClosed }
		listenKafka = func(string, string) (net.Listener, error) { return &appTestListener{}, nil }
		serveKafka = func(net.Listener, int32, []kafkaprotocol.APIVersionRange, kafkaprotocol.MetadataProvider, kafkaprotocol.KafkaBatchAppender, kafkaprotocol.KafkaBatchReader, ...kafkaprotocol.KafkaOffsetLookup) error {
			return nil
		}
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
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
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return nil, fmt.Errorf("database unavailable") }
		listenCalled := false
		listenAndServe = func(*http.Server) error { listenCalled = true; return nil }
		listenKafka = func(string, string) (net.Listener, error) { return &appTestListener{}, nil }
		serveKafka = func(net.Listener, int32, []kafkaprotocol.APIVersionRange, kafkaprotocol.MetadataProvider, kafkaprotocol.KafkaBatchAppender, kafkaprotocol.KafkaBatchReader, ...kafkaprotocol.KafkaOffsetLookup) error {
			return nil
		}
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
			exitProcess = originalExit
		}()

		main()
		if exitCode != 1 || listenCalled {
			t.Errorf("exit code = %d, listen called = %v, want failure before listening", exitCode, listenCalled)
		}
	})

	t.Run("reports Kafka listener startup failures", func(t *testing.T) {
		t.Setenv("DATA_DIR", t.TempDir())
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return sql.Open("minikafka-app-test", "") }
		listenCalled := false
		listenAndServe = func(*http.Server) error { listenCalled = true; return nil }
		listenKafka = func(string, string) (net.Listener, error) { return nil, errors.New("Kafka port unavailable") }
		serveKafka = func(net.Listener, int32, []kafkaprotocol.APIVersionRange, kafkaprotocol.MetadataProvider, kafkaprotocol.KafkaBatchAppender, kafkaprotocol.KafkaBatchReader, ...kafkaprotocol.KafkaOffsetLookup) error {
			return nil
		}
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
			exitProcess = originalExit
		}()

		main()
		if exitCode != 1 || listenCalled {
			t.Errorf("exit code = %d, HTTP listen called = %v, want startup failure before HTTP", exitCode, listenCalled)
		}
	})

	t.Run("metadata provider reports partition inventory errors", func(t *testing.T) {
		dataPath := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("DATA_DIR", dataPath)
		originalPrepare := prepareDatabase
		originalListen := listenAndServe
		originalKafkaListen := listenKafka
		originalKafkaServe := serveKafka
		originalExit := exitProcess
		prepareDatabase = func(context.Context, string) (*sql.DB, error) { return sql.Open("minikafka-app-test", "") }
		listenAndServe = func(*http.Server) error { return http.ErrServerClosed }
		listenKafka = func(string, string) (net.Listener, error) { return &appTestListener{}, nil }
		providerErrors := make(chan error, 1)
		serveKafka = func(_ net.Listener, _ int32, _ []kafkaprotocol.APIVersionRange, metadataProvider kafkaprotocol.MetadataProvider, _ kafkaprotocol.KafkaBatchAppender, _ kafkaprotocol.KafkaBatchReader, _ ...kafkaprotocol.KafkaOffsetLookup) error {
			_, _, err := metadataProvider()
			providerErrors <- err
			return err
		}
		exitCode := 0
		exitProcess = func(code int) { exitCode = code }
		defer func() {
			prepareDatabase = originalPrepare
			listenAndServe = originalListen
			listenKafka = originalKafkaListen
			serveKafka = originalKafkaServe
			exitProcess = originalExit
		}()

		main()
		if err := <-providerErrors; err == nil {
			t.Error("metadataProvider() error = nil, want partition inventory error")
		}
		if exitCode != 0 {
			t.Errorf("exit code = %d, want 0", exitCode)
		}
	})
}

type appTestListener struct {
	closed bool
}

func (*appTestListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *appTestListener) Close() error            { l.closed = true; return nil }
func (*appTestListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9092}
}

type appLogWriter struct {
	messages chan<- string
}

func (w appLogWriter) Write(message []byte) (int, error) {
	w.messages <- string(message)
	return len(message), nil
}
