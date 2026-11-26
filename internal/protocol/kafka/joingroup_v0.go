package kafka

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

type joinGroupProtocolV0 struct {
	Name     string
	Metadata []byte
}

type joinGroupRequestV0 struct {
	GroupID        string
	SessionTimeout int32
	MemberID       string
	ProtocolType   string
	Protocols      []joinGroupProtocolV0
}

// decodeJoinGroupRequestV0 decodes the non-flexible JoinGroup v0 request body.
func decodeJoinGroupRequestV0(body []byte) (joinGroupRequestV0, error) {
	request := joinGroupRequestV0{}
	cursor := 0
	readString := func(field string) (string, error) {
		if len(body)-cursor < 2 {
			return "", fmt.Errorf("decode JoinGroup v0 request: missing %s length", field)
		}
		length := int16(binary.BigEndian.Uint16(body[cursor : cursor+2]))
		cursor += 2
		if length < 0 {
			return "", fmt.Errorf("decode JoinGroup v0 request: negative %s length %d", field, length)
		}
		if int(length) > len(body)-cursor {
			return "", fmt.Errorf("decode JoinGroup v0 request: %s length %d exceeds remaining bytes", field, length)
		}
		value := body[cursor : cursor+int(length)]
		cursor += int(length)
		if !utf8.Valid(value) {
			return "", fmt.Errorf("decode JoinGroup v0 request: %s is not valid UTF-8", field)
		}
		return string(value), nil
	}
	var err error
	if request.GroupID, err = readString("group id"); err != nil {
		return joinGroupRequestV0{}, err
	}
	if len(body)-cursor < 4 {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: missing session timeout")
	}
	request.SessionTimeout = int32(binary.BigEndian.Uint32(body[cursor : cursor+4]))
	cursor += 4
	if request.SessionTimeout < 0 {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: negative session timeout %d", request.SessionTimeout)
	}
	if request.MemberID, err = readString("member id"); err != nil {
		return joinGroupRequestV0{}, err
	}
	if request.ProtocolType, err = readString("protocol type"); err != nil {
		return joinGroupRequestV0{}, err
	}
	if len(body)-cursor < 4 {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: missing protocol count")
	}
	protocolCount := int32(binary.BigEndian.Uint32(body[cursor : cursor+4]))
	cursor += 4
	if protocolCount < 0 {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: negative protocol count %d", protocolCount)
	}
	if int64(protocolCount) > int64((len(body)-cursor)/6) {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: protocol count %d exceeds remaining minimum fields", protocolCount)
	}
	request.Protocols = make([]joinGroupProtocolV0, 0, protocolCount)
	for range int(protocolCount) {
		name, err := readString("protocol name")
		if err != nil {
			return joinGroupRequestV0{}, err
		}
		if len(body)-cursor < 4 {
			return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: missing metadata length")
		}
		metadataLength := int32(binary.BigEndian.Uint32(body[cursor : cursor+4]))
		cursor += 4
		if metadataLength < 0 {
			return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: negative metadata length %d", metadataLength)
		}
		if int64(metadataLength) > int64(len(body)-cursor) {
			return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: metadata length %d exceeds remaining bytes", metadataLength)
		}
		metadata := append([]byte(nil), body[cursor:cursor+int(metadataLength)]...)
		cursor += int(metadataLength)
		request.Protocols = append(request.Protocols, joinGroupProtocolV0{Name: name, Metadata: metadata})
	}
	if cursor != len(body) {
		return joinGroupRequestV0{}, fmt.Errorf("decode JoinGroup v0 request: %d trailing bytes", len(body)-cursor)
	}
	return request, nil
}
