package kafka

import (
	"bytes"
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

type testConn struct {
	reader    *bytes.Reader
	writeErr  error
	zeroWrite bool
}

func (c *testConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *testConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.zeroWrite {
		return 0, nil
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
