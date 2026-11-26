package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenKafkaPartitionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "partition.log")
	log, err := OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
	}
	if log.file == nil || log.nextOffset != 0 {
		t.Errorf("opened log = %+v, want open file and initial offset zero", log)
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close new log: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat new log: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("log permissions = %o, want 600", info.Mode().Perm())
	}
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatalf("write existing log: %v", err)
	}
	log, err = OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog(existing) error = %v", err)
	}
	if _, err := log.file.Write([]byte("+append")); err != nil {
		t.Fatalf("append to opened log: %v", err)
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close existing log: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read appended log: %v", err)
	}
	if string(contents) != "existing+append" {
		t.Errorf("log contents = %q, want %q", contents, "existing+append")
	}
}

func TestOpenKafkaPartitionLogRejectsInvalidPaths(t *testing.T) {
	if log, err := OpenKafkaPartitionLog(""); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(empty) = (%v, %v), want empty-path error", log, err)
	}
	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("file"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	if log, err := OpenKafkaPartitionLog(filepath.Join(parentFile, "partition.log")); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(blocked parent) = (%v, %v), want directory error", log, err)
	}
	directoryPath := t.TempDir()
	if log, err := OpenKafkaPartitionLog(directoryPath); log != nil || err == nil {
		t.Errorf("OpenKafkaPartitionLog(directory) = (%v, %v), want open error", log, err)
	}
}

func TestRecoverKafkaPartitionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partition.log")
	log, err := OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
	}
	if err := RecoverKafkaPartitionLog(log); err != nil {
		t.Fatalf("RecoverKafkaPartitionLog(empty) error = %v", err)
	}
	if log.nextOffset != 0 {
		t.Errorf("empty log next offset = %d, want 0", log.nextOffset)
	}
	first := kafkaBatchForTest(5, 1, 2)
	second := kafkaBatchForTest(7, 0, 1)
	if _, err := log.file.Write(append(first, second...)); err != nil {
		t.Fatalf("write batches: %v", err)
	}
	if err := RecoverKafkaPartitionLog(log); err != nil {
		t.Fatalf("RecoverKafkaPartitionLog(batches) error = %v", err)
	}
	if log.nextOffset != 8 {
		t.Errorf("recovered next offset = %d, want 8", log.nextOffset)
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
}

func TestRecoverKafkaPartitionLogRejectsInvalidLogs(t *testing.T) {
	valid := kafkaBatchForTest(0, 0, 1)
	invalidLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(invalidLength[8:12], 48)
	truncated := append([]byte(nil), valid...)
	truncated[11] = 50
	wrongMagic := append([]byte(nil), valid...)
	wrongMagic[16] = 1
	badCRC := append([]byte(nil), valid...)
	badCRC[21] ^= 1
	negativeBase := kafkaBatchForTest(-1, 0, 1)
	negativeDelta := kafkaBatchForTest(0, -1, 1)
	negativeCount := kafkaBatchForTest(0, 0, -1)
	overflow := kafkaBatchForTest(int64(^uint64(0)>>1), 1, 1)
	decreasing := append(kafkaBatchForTest(5, 0, 1), kafkaBatchForTest(4, 0, 1)...)
	tests := []struct {
		name string
		data []byte
	}{
		{name: "truncated header", data: valid[:60]},
		{name: "invalid batch length", data: invalidLength},
		{name: "truncated batch", data: truncated},
		{name: "wrong magic", data: wrongMagic},
		{name: "invalid CRC32C", data: badCRC},
		{name: "negative base offset", data: negativeBase},
		{name: "negative last offset delta", data: negativeDelta},
		{name: "negative record count", data: negativeCount},
		{name: "offset overflow", data: overflow},
		{name: "decreasing batch offset", data: decreasing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partition.log")
			if err := os.WriteFile(path, test.data, 0o600); err != nil {
				t.Fatalf("write malformed log: %v", err)
			}
			log, err := OpenKafkaPartitionLog(path)
			if err != nil {
				t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
			}
			defer log.file.Close()
			if err := RecoverKafkaPartitionLog(log); err == nil {
				t.Error("RecoverKafkaPartitionLog() error = nil, want malformed-log error")
			}
		})
	}
	if err := RecoverKafkaPartitionLog(nil); err == nil {
		t.Error("RecoverKafkaPartitionLog(nil) error = nil, want nil-log error")
	}
	closed, err := OpenKafkaPartitionLog(filepath.Join(t.TempDir(), "closed.log"))
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog(closed fixture) error = %v", err)
	}
	if err := closed.file.Close(); err != nil {
		t.Fatalf("close fixture: %v", err)
	}
	if err := RecoverKafkaPartitionLog(closed); err == nil {
		t.Error("RecoverKafkaPartitionLog(closed) error = nil, want closed-file error")
	}
}

func TestRecoverKafkaPartitionLogHandlesChecksumReadFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partition.log")
	if err := os.WriteFile(path, kafkaBatchForTest(0, 0, 1), 0o600); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	log, err := OpenKafkaPartitionLog(path)
	if err != nil {
		t.Fatalf("OpenKafkaPartitionLog() error = %v", err)
	}
	defer log.file.Close()
	previous := copyKafkaBatchChecksum
	copyKafkaBatchChecksum = func(io.Writer, io.Reader, int64) (int64, error) {
		return 0, errors.New("injected checksum read failure")
	}
	defer func() { copyKafkaBatchChecksum = previous }()
	if err := RecoverKafkaPartitionLog(log); err == nil {
		t.Error("RecoverKafkaPartitionLog() error = nil, want checksum read error")
	}
}

func TestRecoverKafkaPartitionLogHandlesHeaderReadFailure(t *testing.T) {
	directory := t.TempDir()
	for index := 0; index < 100; index++ {
		path := filepath.Join(directory, string(rune('a'+index%26))+string(rune('A'+index/26)))
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("create directory entry: %v", err)
		}
	}
	file, err := os.Open(directory)
	if err != nil {
		t.Fatalf("open directory as log: %v", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat directory: %v", err)
	}
	if info.Size() < 61 {
		t.Skipf("directory stat size %d is too small to trigger a batch-header read", info.Size())
	}
	if err := RecoverKafkaPartitionLog(&KafkaPartitionLog{file: file}); err == nil {
		t.Error("RecoverKafkaPartitionLog(directory) error = nil, want header-read error")
	}
}

func kafkaBatchForTest(baseOffset int64, lastOffsetDelta, recordCount int32) []byte {
	batch := make([]byte, 61)
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], 49)
	binary.BigEndian.PutUint32(batch[12:16], 0)
	batch[16] = 2
	binary.BigEndian.PutUint32(batch[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))
	checksum := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], checksum)
	return batch
}
