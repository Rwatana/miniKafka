package kafka

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestServeAPIVersionsV3ConnectionProcessesOrderedFrames(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	go func() { done <- ServeAPIVersionsV3Connection(server, 1024, supported) }()

	requests := appendFrame(nil, apiVersionsRequestFrameForTest(1))
	requests = appendFrame(requests, apiVersionsRequestFrameForTest(2))
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(requests)
		writeDone <- err
	}()

	wantResponses := [][]byte{
		{0, 0, 0, 18, 0, 0, 0, 1, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0},
		{0, 0, 0, 18, 0, 0, 0, 2, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0},
	}
	for _, want := range wantResponses {
		got, err := ReadRequestFrame(client, 1024)
		if err != nil {
			t.Fatalf("ReadRequestFrame() error = %v", err)
		}
		if !bytes.Equal(got, want[4:]) {
			t.Errorf("response body = %x, want %x", got, want[4:])
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write requests: %v", err)
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeAPIVersionsV3Connection() error = %v, want clean EOF", err)
	}
	_ = server.Close()
}

func TestServeAPIVersionsV3ConnectionReturnsProtocolAndReadErrors(t *testing.T) {
	t.Run("partial frame", func(t *testing.T) {
		conn := &testConn{reader: bytes.NewReader([]byte{0, 0})}
		if err := ServeAPIVersionsV3Connection(conn, 1024, nil); err == nil {
			t.Error("ServeAPIVersionsV3Connection() error = nil, want truncated-frame error")
		}
	})

	t.Run("invalid request", func(t *testing.T) {
		server, client := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- ServeAPIVersionsV3Connection(server, 1024, nil) }()
		input := appendFrame(nil, []byte{0, 17, 0, 3, 0, 0, 0, 1, 1, 0, 1, 1, 0})
		writeDone := make(chan error, 1)
		go func() { _, err := client.Write(input); writeDone <- err }()
		<-writeDone
		err := <-done
		_ = client.Close()
		if err == nil {
			t.Error("ServeAPIVersionsV3Connection() error = nil, want protocol error")
		}
		_ = server.Close()
	})
}

func TestServeAPIVersionsV3ConnectionHandlesWriteFailures(t *testing.T) {
	request := appendFrame(nil, apiVersionsRequestFrameForTest(1))
	for name, conn := range map[string]*testConn{
		"write error": {reader: bytes.NewReader(request), writeErr: io.ErrClosedPipe},
		"zero write":  {reader: bytes.NewReader(request), zeroWrite: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := ServeAPIVersionsV3Connection(conn, 1024, []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}})
			if err == nil {
				t.Error("ServeAPIVersionsV3Connection() error = nil, want write error")
			}
		})
	}
}

func TestServeKafkaConnectionProcessesMixedAPIs(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	metadataCalls := 0
	metadataProvider := func() ([]MetadataBroker, []MetadataTopic, error) {
		metadataCalls++
		return nil, nil, nil
	}
	go func() { done <- ServeKafkaConnection(server, 1024, supported, metadataProvider) }()
	requests := appendFrame(nil, apiVersionsRequestFrameForTest(1))
	requests = appendFrame(requests, metadataRequestFrameV0(3, 0, 2, []byte{0, 0, 0, 0}))
	writeDone := make(chan error, 1)
	go func() { _, err := client.Write(requests); writeDone <- err }()

	wantResponses := [][]byte{
		{0, 0, 0, 1, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0},
		{0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, want := range wantResponses {
		got, err := ReadRequestFrame(client, 1024)
		if err != nil {
			t.Fatalf("ReadRequestFrame() error = %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("response = %x, want %x", got, want)
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write requests: %v", err)
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafkaConnection() error = %v, want clean EOF", err)
	}
	if metadataCalls != 1 {
		t.Errorf("metadata provider calls = %d, want only the Metadata request call", metadataCalls)
	}
	_ = server.Close()
}

func TestServeKafkaConnectionProcessesProduceV3(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	appendCalls := 0
	batch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	expectedBatchSize := len(batch)
	go func() {
		done <- ServeKafkaConnection(server, 1024, nil, nil, func(topic string, partition int, batch []byte) (int64, error) {
			appendCalls++
			if topic != "orders" || partition != 0 || len(batch) != expectedBatchSize {
				t.Errorf("appender arguments = (%q, %d, %d bytes), want orders, 0, %d bytes", topic, partition, len(batch), expectedBatchSize)
			}
			return 12, nil
		})
	}()
	request := appendFrame(nil, produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: batch}}}}))
	writeDone := make(chan error, 1)
	go func() { _, err := client.Write(request); writeDone <- err }()
	response, err := ReadRequestFrame(client, 1024)
	if err != nil {
		t.Fatalf("ReadRequestFrame() error = %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write Produce request: %v", err)
	}
	want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{{Index: 0, BaseOffset: 12, LogAppendTimeMs: -1}}}}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, want[4:]) {
		t.Errorf("Produce response body = %x, want %x", response, want[4:])
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafkaConnection() error = %v, want clean EOF", err)
	}
	_ = server.Close()
	if appendCalls != 1 {
		t.Errorf("append calls = %d, want 1", appendCalls)
	}
}

func TestServeKafkaConnectionSkipsProduceAcksZeroResponse(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	appendCalls := 0
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	go func() {
		done <- ServeKafkaConnection(server, 1024, supported, nil, func(string, int, []byte) (int64, error) {
			appendCalls++
			return 0, nil
		})
	}()
	batch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	requests := appendFrame(nil, produceRequestFrameV3ForTest(nil, 0, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: batch}}}}))
	requests = appendFrame(requests, apiVersionsRequestFrameForTest(19))
	writeDone := make(chan error, 1)
	go func() { _, err := client.Write(requests); writeDone <- err }()
	response, err := ReadRequestFrame(client, 1024)
	if err != nil {
		t.Fatalf("ReadRequestFrame() after acks=0 error = %v", err)
	}
	if len(response) < 4 || int32(response[3]) != 19 {
		t.Errorf("response after acks=0 = %x, want following ApiVersions correlation ID 19", response)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write requests: %v", err)
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafkaConnection() error = %v, want clean EOF", err)
	}
	_ = server.Close()
	if appendCalls != 1 {
		t.Errorf("append calls = %d, want 1", appendCalls)
	}
}

func TestServeKafkaConnectionReturnsReadAndDispatchErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		input  []byte
		reader *bytes.Reader
	}{
		{name: "partial frame", reader: bytes.NewReader([]byte{0, 0})},
		{name: "unsupported API", input: appendFrame(nil, []byte{0, 99})},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := test.reader
			if reader == nil {
				reader = bytes.NewReader(test.input)
			}
			if err := ServeKafkaConnection(&testConn{reader: reader}, 1024, nil, nil); err == nil {
				t.Error("ServeKafkaConnection() error = nil, want error")
			}
		})
	}
}

func TestServeKafkaConnectionHandlesWrites(t *testing.T) {
	request := appendFrame(nil, apiVersionsRequestFrameForTest(1))
	for name, conn := range map[string]*testConn{
		"write error":   {reader: bytes.NewReader(request), writeErr: io.ErrClosedPipe},
		"zero write":    {reader: bytes.NewReader(request), zeroWrite: true},
		"partial write": {reader: bytes.NewReader(request), writeLimit: 1},
	} {
		t.Run(name, func(t *testing.T) {
			err := ServeKafkaConnection(conn, 1024, []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}, nil)
			if name == "partial write" {
				if err != nil {
					t.Errorf("ServeKafkaConnection() error = %v, want clean EOF", err)
				}
			} else if err == nil {
				t.Error("ServeKafkaConnection() error = nil, want write error")
			}
		})
	}
}

func TestServeKafkaConnectionRefreshesMetadataPerRequest(t *testing.T) {
	server, client := net.Pipe()
	done := make(chan error, 1)
	metadataCalls := 0
	metadataProvider := func() ([]MetadataBroker, []MetadataTopic, error) {
		metadataCalls++
		if metadataCalls == 1 {
			return nil, []MetadataTopic{{Name: "orders"}}, nil
		}
		return nil, []MetadataTopic{{Name: "events"}}, nil
	}
	go func() { done <- ServeKafkaConnection(server, 1024, nil, metadataProvider) }()

	for correlationID, topicName := range []string{"orders", "events"} {
		request := appendFrame(nil, metadataRequestFrameV0(3, 0, int32(correlationID+1), []byte{0, 0, 0, 0}))
		writeDone := make(chan error, 1)
		go func() { _, err := client.Write(request); writeDone <- err }()
		response, err := ReadRequestFrame(client, 1024)
		if err != nil {
			t.Fatalf("ReadRequestFrame() error = %v", err)
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("write request: %v", err)
		}
		if got := string(response[16 : 16+len(topicName)]); got != topicName {
			t.Errorf("response topic = %q, want %q", got, topicName)
		}
	}
	_ = client.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafkaConnection() error = %v, want clean EOF", err)
	}
	_ = server.Close()
	if metadataCalls != 2 {
		t.Errorf("metadata provider calls = %d, want 2", metadataCalls)
	}
}

func TestServeKafkaConnectionReturnsMetadataProviderErrors(t *testing.T) {
	input := appendFrame(nil, metadataRequestFrameV0(3, 0, 1, []byte{0, 0, 0, 0}))
	metadataProvider := func() ([]MetadataBroker, []MetadataTopic, error) {
		return nil, nil, errors.New("metadata unavailable")
	}
	if err := ServeKafkaConnection(&testConn{reader: bytes.NewReader(input)}, 1024, nil, metadataProvider); err == nil {
		t.Error("ServeKafkaConnection() error = nil, want metadata provider error")
	}
}

type testConn struct {
	reader     *bytes.Reader
	writeErr   error
	zeroWrite  bool
	writeLimit int
}

func (c *testConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *testConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.zeroWrite {
		return 0, nil
	}
	if c.writeLimit > 0 && len(p) > c.writeLimit {
		return c.writeLimit, nil
	}
	return len(p), nil
}
func (*testConn) Close() error                     { return nil }
func (*testConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*testConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*testConn) SetDeadline(time.Time) error      { return nil }
func (*testConn) SetReadDeadline(time.Time) error  { return nil }
func (*testConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func apiVersionsRequestFrameForTest(correlationID byte) []byte {
	return []byte{0, 18, 0, 3, 0, 0, 0, correlationID, 1, 0, 5, 'j', 'a', 'v', 'a', 5, '1', '.', '2', '0', 0}
}
