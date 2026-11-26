package kafka

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeRequestHeaderV2(t *testing.T) {
	t.Run("decodes flexible fields, tags, and API body", func(t *testing.T) {
		frame := requestHeaderV2Fixture(18, 4, 0x01020304, []byte{7, 'c', 'l', 'i', 'e', 'n', 't'}, []byte{1, 0, 1, 0xaa}, []byte{0x55, 0x66})
		header, body, err := DecodeRequestHeaderV2(frame)
		if err != nil {
			t.Fatalf("DecodeRequestHeaderV2() error = %v", err)
		}
		if header.APIKey != 18 || header.APIVersion != 4 || header.CorrelationID != 0x01020304 || header.ClientID == nil || *header.ClientID != "client" {
			t.Errorf("header = %+v, want decoded flexible header", header)
		}
		if !reflect.DeepEqual(body, []byte{0x55, 0x66}) {
			t.Errorf("body = %x, want 55 66", body)
		}
	})

	t.Run("preserves null and empty client IDs", func(t *testing.T) {
		for name, clientID := range map[string][]byte{"null": {0}, "empty": {1}} {
			frame := requestHeaderV2Fixture(18, 3, 9, clientID, []byte{0}, []byte{0})
			header, body, err := DecodeRequestHeaderV2(frame)
			if err != nil {
				t.Fatalf("DecodeRequestHeaderV2(%s) error = %v", name, err)
			}
			if name == "null" && header.ClientID != nil {
				t.Errorf("null client ID = %v, want nil", *header.ClientID)
			}
			if name == "empty" && (header.ClientID == nil || *header.ClientID != "") {
				t.Errorf("empty client ID = %v, want non-nil empty string", header.ClientID)
			}
			if !reflect.DeepEqual(body, []byte{0}) {
				t.Errorf("body = %v, want [0]", body)
			}
		}
	})

	t.Run("rejects truncated fields and malformed tags", func(t *testing.T) {
		fixed := make([]byte, 8)
		binary.BigEndian.PutUint16(fixed[0:2], 18)
		binary.BigEndian.PutUint16(fixed[2:4], 4)
		for name, frame := range map[string][]byte{
			"fixed header":   fixed[:7],
			"client ID":      fixed,
			"tag buffer":     append(append([]byte(nil), fixed...), 1),
			"malformed tags": append(append(append([]byte(nil), fixed...), 0), []byte{2, 2, 0, 1, 0}...),
		} {
			if _, _, err := DecodeRequestHeaderV2(frame); err == nil {
				t.Errorf("%s error = nil, want error", name)
			}
		}
	})
}

func requestHeaderV2Fixture(apiKey, apiVersion int16, correlationID int32, clientID, tags, body []byte) []byte {
	frame := make([]byte, 8)
	binary.BigEndian.PutUint16(frame[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(frame[2:4], uint16(apiVersion))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	frame = append(frame, clientID...)
	frame = append(frame, tags...)
	return append(frame, body...)
}
