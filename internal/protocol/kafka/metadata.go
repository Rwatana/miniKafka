package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// decodeMetadataRequestV0 decodes the legacy topic array in a Metadata v0 request.
func decodeMetadataRequestV0(body []byte) ([]string, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("decode Metadata v0 request: missing topic count")
	}
	count := int32(binary.BigEndian.Uint32(body[:4]))
	if count < 0 {
		return nil, fmt.Errorf("decode Metadata v0 request: negative topic count %d", count)
	}
	if int64(count) > int64((len(body)-4)/2) {
		return nil, fmt.Errorf("decode Metadata v0 request: topic count %d exceeds remaining minimum fields", count)
	}
	topics := make([]string, 0)
	cursor := 4
	for index := int32(0); index < count; index++ {
		length := int16(binary.BigEndian.Uint16(body[cursor : cursor+2]))
		cursor += 2
		if length < 0 {
			return nil, fmt.Errorf("decode Metadata v0 request: negative topic %d length %d", index, length)
		}
		end := cursor + int(length)
		if end > len(body) {
			return nil, fmt.Errorf("decode Metadata v0 request: topic %d length %d exceeds remaining bytes %d", index, length, len(body)-cursor)
		}
		nameBytes := body[cursor:end]
		if !utf8.Valid(nameBytes) {
			return nil, fmt.Errorf("decode Metadata v0 request: topic %d name is not valid UTF-8", index)
		}
		topics = append(topics, string(nameBytes))
		cursor = end
	}
	if cursor != len(body) {
		return nil, fmt.Errorf("decode Metadata v0 request: %d trailing bytes", len(body)-cursor)
	}
	return topics, nil
}
