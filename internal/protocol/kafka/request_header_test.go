package kafka

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeRequestHeaderV1(t *testing.T) {
	t.Run("decodes fields and leaves the API body", func(t *testing.T) {
		frame := requestHeaderFixture(18, 2, 0x12345678, "go-client", []byte{9, 8})
		header, body, err := DecodeRequestHeaderV1(frame)
		if err != nil {
			t.Fatalf("DecodeRequestHeaderV1() error = %v", err)
		}
		if header.APIKey != 18 || header.APIVersion != 2 || header.CorrelationID != 0x12345678 || header.ClientID == nil || *header.ClientID != "go-client" {
			t.Errorf("header = %+v, want decoded ApiVersions header", header)
		}
		if !reflect.DeepEqual(body, []byte{9, 8}) {
			t.Errorf("body = %v, want [9 8]", body)
		}
	})

	t.Run("preserves null and empty client IDs", func(t *testing.T) {
		nullFrame := requestHeaderFixture(18, 0, 7, "", []byte{1})
		binary.BigEndian.PutUint16(nullFrame[8:10], 0xffff)
		nullHeader, nullBody, err := DecodeRequestHeaderV1(nullFrame)
		if err != nil || nullHeader.ClientID != nil || !reflect.DeepEqual(nullBody, []byte{1}) {
			t.Errorf("null ClientID decode = (%+v, %v, %v), want nil ID and body [1]", nullHeader, nullBody, err)
		}

		emptyFrame := requestHeaderFixture(18, 0, 7, "", []byte{2})
		emptyHeader, _, err := DecodeRequestHeaderV1(emptyFrame)
		if err != nil || emptyHeader.ClientID == nil || *emptyHeader.ClientID != "" {
			t.Errorf("empty ClientID decode = (%+v, %v), want non-nil empty ID", emptyHeader, err)
		}
	})

	t.Run("rejects truncated fixed fields and client ID", func(t *testing.T) {
		valid := requestHeaderFixture(18, 0, 7, "x", nil)
		for name, frame := range map[string][]byte{
			"fixed header":  valid[:7],
			"client length": valid[:9],
			"client bytes":  valid[:len(valid)-1],
		} {
			if _, _, err := DecodeRequestHeaderV1(frame); err == nil {
				t.Errorf("%s error = nil, want error", name)
			}
		}
	})

	t.Run("rejects invalid client ID length and UTF-8", func(t *testing.T) {
		negative := requestHeaderFixture(18, 0, 7, "", nil)
		binary.BigEndian.PutUint16(negative[8:10], 0xfffe)
		invalidUTF8 := requestHeaderFixture(18, 0, 7, "", []byte{0xff})
		binary.BigEndian.PutUint16(invalidUTF8[8:10], 1)
		for name, frame := range map[string][]byte{"negative length": negative, "invalid UTF-8": invalidUTF8} {
			if _, _, err := DecodeRequestHeaderV1(frame); err == nil {
				t.Errorf("%s error = nil, want error", name)
			}
		}
	})
}

func requestHeaderFixture(apiKey, apiVersion int16, correlationID int32, clientID string, body []byte) []byte {
	frame := make([]byte, 10+len(clientID)+len(body))
	binary.BigEndian.PutUint16(frame[0:2], uint16(apiKey))
	binary.BigEndian.PutUint16(frame[2:4], uint16(apiVersion))
	binary.BigEndian.PutUint32(frame[4:8], uint32(correlationID))
	binary.BigEndian.PutUint16(frame[8:10], uint16(len(clientID)))
	copy(frame[10:], clientID)
	copy(frame[10+len(clientID):], body)
	return frame
}
