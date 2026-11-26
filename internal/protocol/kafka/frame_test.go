package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestReadRequestFrame(t *testing.T) {
	t.Run("rejects nil reader", func(t *testing.T) {
		if _, err := ReadRequestFrame(nil, 8); err == nil {
			t.Error("ReadRequestFrame(nil) error = nil, want error")
		}
	})

	t.Run("reads exact bodies from consecutive frames", func(t *testing.T) {
		stream := appendFrame(nil, []byte{1, 2, 3})
		stream = appendFrame(stream, []byte{4, 5})
		reader := bytes.NewReader(stream)
		for _, want := range [][]byte{{1, 2, 3}, {4, 5}} {
			got, err := ReadRequestFrame(reader, 8)
			if err != nil {
				t.Fatalf("ReadRequestFrame() error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("frame = %v, want %v", got, want)
			}
		}
		if _, err := ReadRequestFrame(reader, 8); !errors.Is(err, io.EOF) {
			t.Errorf("ReadRequestFrame() at boundary error = %v, want EOF", err)
		}
	})

	t.Run("rejects invalid maximum", func(t *testing.T) {
		for _, max := range []int32{0, -1} {
			if _, err := ReadRequestFrame(bytes.NewReader(nil), max); err == nil {
				t.Errorf("ReadRequestFrame(max=%d) error = nil, want error", max)
			}
		}
	})

	t.Run("rejects non-positive and oversized lengths", func(t *testing.T) {
		for _, length := range []int32{0, -1, 9} {
			reader := bytes.NewReader(prefix(length))
			if _, err := ReadRequestFrame(reader, 8); err == nil {
				t.Errorf("ReadRequestFrame(length=%d) error = nil, want error", length)
			}
			if reader.Len() != 0 {
				t.Errorf("length %d consumed body bytes, want prefix-only rejection", length)
			}
		}
	})

	t.Run("preserves clean EOF before a frame", func(t *testing.T) {
		if _, err := ReadRequestFrame(bytes.NewReader(nil), 8); !errors.Is(err, io.EOF) {
			t.Errorf("ReadRequestFrame() error = %v, want EOF", err)
		}
	})

	t.Run("reports truncated prefix and body", func(t *testing.T) {
		for name, input := range map[string][]byte{
			"prefix": {0, 0},
			"body":   prefix(3),
		} {
			if _, err := ReadRequestFrame(bytes.NewReader(input), 8); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("%s error = %v, want unexpected EOF", name, err)
			}
		}
	})
}

func TestEncodeResponseFrame(t *testing.T) {
	t.Run("prefixes body with big-endian size", func(t *testing.T) {
		got, err := encodeResponseFrame([]byte{0xaa, 0xbb, 0xcc}, 8)
		if err != nil {
			t.Fatalf("encodeResponseFrame() error = %v", err)
		}
		want := []byte{0, 0, 0, 3, 0xaa, 0xbb, 0xcc}
		if !bytes.Equal(got, want) {
			t.Errorf("frame = %x, want %x", got, want)
		}
	})

	t.Run("accepts an empty body", func(t *testing.T) {
		got, err := encodeResponseFrame(nil, 8)
		if err != nil {
			t.Fatalf("encodeResponseFrame() error = %v", err)
		}
		if !bytes.Equal(got, []byte{0, 0, 0, 0}) {
			t.Errorf("frame = %x, want zero-length size prefix", got)
		}
	})

	t.Run("rejects invalid bounds", func(t *testing.T) {
		for _, max := range []int32{0, -1} {
			if _, err := encodeResponseFrame(nil, max); err == nil {
				t.Errorf("encodeResponseFrame(max=%d) error = nil, want error", max)
			}
		}
	})

	t.Run("rejects body above configured bound", func(t *testing.T) {
		if _, err := encodeResponseFrame([]byte{1, 2, 3}, 2); err == nil {
			t.Error("encodeResponseFrame() error = nil, want oversized-body error")
		}
	})
}

func appendFrame(stream, body []byte) []byte {
	stream = append(stream, prefix(int32(len(body)))...)
	return append(stream, body...)
}

func prefix(size int32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(size))
	return encoded[:]
}
