package kafka

import (
	"bytes"
	"errors"
	"net"
	"testing"
)

func TestServeAPIVersionsV3AcceptsTCPClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	done := make(chan error, 1)
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	go func() { done <- ServeAPIVersionsV3(listener, 1024, supported) }()

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	request := appendFrame(nil, apiVersionsRequestFrameForTest(9))
	if _, err := client.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	got, err := ReadRequestFrame(client, 1024)
	if err != nil {
		t.Fatalf("ReadRequestFrame() error = %v", err)
	}
	want := []byte{0, 0, 0, 9, 0, 0, 2, 0, 18, 0, 0, 0, 3, 0, 0, 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Errorf("response = %x, want %x", got, want)
	}
	_ = client.Close()
	_ = listener.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeAPIVersionsV3() error = %v, want clean listener close", err)
	}
}

func TestServeAPIVersionsV3ReturnsAcceptErrors(t *testing.T) {
	wantErr := errors.New("accept failed")
	listener := &errorListener{err: wantErr}
	if err := ServeAPIVersionsV3(listener, 1024, nil); !errors.Is(err, wantErr) {
		t.Errorf("ServeAPIVersionsV3() error = %v, want %v", err, wantErr)
	}
}

type errorListener struct {
	err error
}

func (l *errorListener) Accept() (net.Conn, error) { return nil, l.err }
func (*errorListener) Close() error                { return nil }
func (*errorListener) Addr() net.Addr              { return listenerAddr{} }

type listenerAddr struct{}

func (listenerAddr) Network() string { return "test" }
func (listenerAddr) String() string  { return "test" }

func TestServeAPIVersionsV3ReturnsCleanlyWhenListenerIsClosed(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	if err := ServeAPIVersionsV3(listener, 1024, nil); err != nil {
		t.Errorf("ServeAPIVersionsV3() error = %v, want nil", err)
	}
}

func TestServeKafkaAcceptsMixedAPIClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	metadataCalls := 0
	metadataProvider := func() ([]MetadataBroker, []MetadataTopic, error) {
		metadataCalls++
		return nil, nil, nil
	}
	done := make(chan error, 1)
	supported := []APIVersionRange{{APIKey: 18, MinVersion: 0, MaxVersion: 3}}
	go func() { done <- ServeKafka(listener, 1024, supported, metadataProvider) }()

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	for _, request := range [][]byte{
		apiVersionsRequestFrameForTest(9),
		metadataRequestFrameV0(3, 0, 10, []byte{0, 0, 0, 0}),
	} {
		if _, err := client.Write(appendFrame(nil, request)); err != nil {
			t.Fatalf("write request: %v", err)
		}
		if _, err := ReadRequestFrame(client, 1024); err != nil {
			t.Fatalf("ReadRequestFrame() error = %v", err)
		}
	}
	if metadataCalls != 1 {
		t.Errorf("metadata provider calls = %d, want 1", metadataCalls)
	}
	_ = client.Close()
	_ = listener.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafka() error = %v, want clean listener close", err)
	}
}

func TestServeKafkaAcceptsProduceV3Client(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- ServeKafka(listener, 1024, nil, nil, func(topic string, partition int, batch []byte) (int64, error) {
			if topic != "orders" || partition != 0 || len(batch) < 61 {
				t.Errorf("appender arguments = (%q, %d, %d bytes), want orders, 0, Kafka batch", topic, partition, len(batch))
			}
			return 23, nil
		})
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	batch := recordBatchV2ForTest(1, 0, recordV2ForTest(kafkaRecordV2{Headers: []recordHeaderV2{}}))
	request := appendFrame(nil, produceRequestFrameV3ForTest(nil, 1, []produceTopicData{{Name: "orders", Partitions: []producePartitionData{{Index: 0, Records: batch}}}}))
	if _, err := client.Write(request); err != nil {
		t.Fatalf("write Produce request: %v", err)
	}
	got, err := ReadRequestFrame(client, 1024)
	if err != nil {
		t.Fatalf("ReadRequestFrame() error = %v", err)
	}
	want, err := encodeProduceResponseFrameV3(42, produceResponseV3{Topics: []produceTopicResponseV3{{Name: "orders", Partitions: []producePartitionResponseV3{{Index: 0, BaseOffset: 23, LogAppendTimeMs: -1}}}}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want[4:]) {
		t.Errorf("Produce response = %x, want %x", got, want[4:])
	}
	_ = client.Close()
	_ = listener.Close()
	if err := <-done; err != nil {
		t.Errorf("ServeKafka() error = %v, want clean listener close", err)
	}
}

func TestServeKafkaReturnsAcceptErrors(t *testing.T) {
	wantErr := errors.New("accept failed")
	listener := &errorListener{err: wantErr}
	if err := ServeKafka(listener, 1024, nil, nil); !errors.Is(err, wantErr) {
		t.Errorf("ServeKafka() error = %v, want %v", err, wantErr)
	}
}

func TestServeKafkaReturnsCleanlyWhenListenerIsClosed(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	if err := ServeKafka(listener, 1024, nil, nil); err != nil {
		t.Errorf("ServeKafka() error = %v, want nil", err)
	}
}
