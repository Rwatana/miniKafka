package kafka

import (
	"reflect"
	"testing"
)

func TestDecodeRecordV2(t *testing.T) {
	encoded := recordV2ForTest(kafkaRecordV2{
		Attributes:     1,
		TimestampDelta: -5,
		OffsetDelta:    2,
		Key:            []byte("key"),
		Value:          []byte("value"),
		Headers: []recordHeaderV2{
			{Key: "source", Value: []byte("api")},
			{Key: "nullable", Value: nil},
		},
	})
	got, consumed, err := decodeRecordV2(append(append([]byte(nil), encoded...), 0xff))
	if err != nil {
		t.Fatalf("decodeRecordV2() error = %v", err)
	}
	want := kafkaRecordV2{
		Attributes:     1,
		TimestampDelta: -5,
		OffsetDelta:    2,
		Key:            []byte("key"),
		Value:          []byte("value"),
		Headers: []recordHeaderV2{
			{Key: "source", Value: []byte("api")},
			{Key: "nullable", Value: nil},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decodeRecordV2() = %+v, want %+v", got, want)
	}
	if consumed != len(encoded) {
		t.Errorf("consumed = %d, want %d", consumed, len(encoded))
	}

	empty, consumed, err := decodeRecordV2(recordV2ForTest(kafkaRecordV2{}))
	if err != nil {
		t.Fatalf("decodeRecordV2(empty fields) error = %v", err)
	}
	if empty.Key != nil || empty.Value != nil || len(empty.Headers) != 0 || consumed == 0 {
		t.Errorf("empty record = %+v, consumed = %d", empty, consumed)
	}
}

func TestDecodeRecordV2RejectsMalformedRecord(t *testing.T) {
	keyNull := []byte{0, 0, 0, 1}
	keyNullValueNull := append(append([]byte(nil), keyNull...), 1)
	validBody := append(append([]byte(nil), keyNullValueNull...), 0)
	negativeKeyLength := append(append([]byte{0, 0, 0}, 3), 0)
	tooLongKeyLength := []byte{0, 0, 0, 4}
	negativeValueLength := append(append([]byte(nil), keyNull...), 3)
	tooLongValueLength := append(append([]byte(nil), keyNull...), 4)
	missingHeaderCount := append(append([]byte(nil), keyNull...), 1)
	negativeHeaderCount := append(append([]byte(nil), keyNullValueNull...), 1)
	tooManyHeaders := append(append([]byte(nil), keyNullValueNull...), 2)
	negativeHeaderKeyLength := append(append([]byte(nil), keyNullValueNull...), 2, 1)
	tooLongHeaderKey := append(append([]byte(nil), keyNullValueNull...), 2, 4, 'x')
	invalidHeaderKeyUTF8 := append(append([]byte(nil), keyNullValueNull...), 2, 2, 0xff, 1)
	negativeHeaderValueLength := append(append([]byte(nil), keyNullValueNull...), 2, 2, 'x', 3)
	tooLongHeaderValue := append(append([]byte(nil), keyNullValueNull...), 2, 2, 'x', 4)
	missingHeaderValueLength := append(append([]byte(nil), keyNullValueNull...), 2, 2, 'x')
	trailingBytes := append(append([]byte(nil), validBody...), 0xff)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "truncated record length varint", data: []byte{0x80}},
		{name: "negative record length", data: signedVarintForTest(-1)},
		{name: "record length exceeds available bytes", data: append(signedVarintForTest(10), 0)},
		{name: "missing attributes", data: wrapRecordDataForTest(nil)},
		{name: "missing timestamp delta", data: wrapRecordDataForTest([]byte{0})},
		{name: "truncated timestamp delta", data: wrapRecordDataForTest([]byte{0, 0x80})},
		{name: "missing offset delta", data: wrapRecordDataForTest([]byte{0, 0})},
		{name: "invalid key length", data: wrapRecordDataForTest(negativeKeyLength)},
		{name: "key length exceeds available bytes", data: wrapRecordDataForTest(tooLongKeyLength)},
		{name: "invalid value length", data: wrapRecordDataForTest(negativeValueLength)},
		{name: "value length exceeds available bytes", data: wrapRecordDataForTest(tooLongValueLength)},
		{name: "missing header count", data: wrapRecordDataForTest(missingHeaderCount)},
		{name: "negative header count", data: wrapRecordDataForTest(negativeHeaderCount)},
		{name: "header count exceeds remaining fields", data: wrapRecordDataForTest(tooManyHeaders)},
		{name: "negative header key length", data: wrapRecordDataForTest(negativeHeaderKeyLength)},
		{name: "header key length exceeds remaining bytes", data: wrapRecordDataForTest(tooLongHeaderKey)},
		{name: "invalid UTF-8 header key", data: wrapRecordDataForTest(invalidHeaderKeyUTF8)},
		{name: "invalid header value length", data: wrapRecordDataForTest(negativeHeaderValueLength)},
		{name: "header value length exceeds remaining bytes", data: wrapRecordDataForTest(tooLongHeaderValue)},
		{name: "truncated header value length", data: wrapRecordDataForTest(missingHeaderValueLength)},
		{name: "trailing bytes inside record", data: wrapRecordDataForTest(trailingBytes)},
		{name: "invalid header key varint", data: wrapRecordDataForTest(append(append([]byte(nil), keyNullValueNull...), 2, 0x80))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := decodeRecordV2(test.data); err == nil {
				t.Error("decodeRecordV2() error = nil, want malformed-record error")
			}
		})
	}
}

func recordV2ForTest(record kafkaRecordV2) []byte {
	data := []byte{byte(record.Attributes)}
	data = append(data, signedVarlongForTest(record.TimestampDelta)...)
	data = append(data, signedVarintForTest(record.OffsetDelta)...)
	data = appendNullableRecordBytesForTest(data, record.Key)
	data = appendNullableRecordBytesForTest(data, record.Value)
	data = append(data, signedVarintForTest(int32(len(record.Headers)))...)
	for _, header := range record.Headers {
		data = append(data, signedVarintForTest(int32(len(header.Key)))...)
		data = append(data, header.Key...)
		data = appendNullableRecordBytesForTest(data, header.Value)
	}
	return wrapRecordDataForTest(data)
}

func appendNullableRecordBytesForTest(data, value []byte) []byte {
	if value == nil {
		return append(data, signedVarintForTest(-1)...)
	}
	data = append(data, signedVarintForTest(int32(len(value)))...)
	return append(data, value...)
}

func wrapRecordDataForTest(data []byte) []byte {
	return append(signedVarintForTest(int32(len(data))), data...)
}
