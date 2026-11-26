package kafka

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

type joinGroupMemberV0 struct {
	MemberID string
	Metadata []byte
}

type joinGroupResponseV0 struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	Leader       string
	MemberID     string
	Members      []joinGroupMemberV0
}

// encodeJoinGroupResponseV0 encodes a non-flexible JoinGroup v0 response body.
func encodeJoinGroupResponseV0(response joinGroupResponseV0) ([]byte, error) {
	encoded := make([]byte, 0)
	appendInt16 := func(value int16) {
		var field [2]byte
		binary.BigEndian.PutUint16(field[:], uint16(value))
		encoded = append(encoded, field[:]...)
	}
	appendInt32 := func(value int32) {
		var field [4]byte
		binary.BigEndian.PutUint32(field[:], uint32(value))
		encoded = append(encoded, field[:]...)
	}
	appendString := func(field, value string) error {
		if !utf8.ValidString(value) {
			return fmt.Errorf("encode JoinGroup v0 response: %s is not valid UTF-8", field)
		}
		if len(value) > math.MaxInt16 {
			return fmt.Errorf("encode JoinGroup v0 response: %s length %d exceeds INT16 maximum", field, len(value))
		}
		appendInt16(int16(len(value)))
		encoded = append(encoded, value...)
		return nil
	}

	appendInt16(response.ErrorCode)
	appendInt32(response.GenerationID)
	if err := appendString("protocol name", response.ProtocolName); err != nil {
		return nil, err
	}
	if err := appendString("leader", response.Leader); err != nil {
		return nil, err
	}
	if err := appendString("member id", response.MemberID); err != nil {
		return nil, err
	}
	appendInt32(int32(len(response.Members)))
	for _, member := range response.Members {
		if err := appendString("member id", member.MemberID); err != nil {
			return nil, err
		}
		appendInt32(int32(len(member.Metadata)))
		encoded = append(encoded, member.Metadata...)
	}
	return encoded, nil
}
