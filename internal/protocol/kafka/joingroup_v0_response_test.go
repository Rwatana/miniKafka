package kafka

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestEncodeJoinGroupResponseV0(t *testing.T) {
	response := joinGroupResponseV0{
		ErrorCode:    0,
		GenerationID: 12,
		ProtocolName: "range",
		Leader:       "member-1",
		MemberID:     "member-1",
		Members:      []joinGroupMemberV0{{MemberID: "member-1", Metadata: []byte{1, 2, 3}}},
	}
	got, err := encodeJoinGroupResponseV0(response)
	if err != nil {
		t.Fatalf("encodeJoinGroupResponseV0() error = %v", err)
	}
	want := binary.BigEndian.AppendUint16(nil, 0)
	want = binary.BigEndian.AppendUint32(want, 12)
	want = appendJoinGroupStringV0ForTest(want, "range")
	want = appendJoinGroupStringV0ForTest(want, "member-1")
	want = appendJoinGroupStringV0ForTest(want, "member-1")
	want = binary.BigEndian.AppendUint32(want, 1)
	want = appendJoinGroupStringV0ForTest(want, "member-1")
	want = binary.BigEndian.AppendUint32(want, 3)
	want = append(want, 1, 2, 3)
	if !bytes.Equal(got, want) {
		t.Errorf("encodeJoinGroupResponseV0() = %x, want %x", got, want)
	}
}

func TestEncodeJoinGroupResponseV0RejectsInvalidStrings(t *testing.T) {
	for _, test := range []struct {
		name     string
		response joinGroupResponseV0
	}{
		{name: "invalid protocol name UTF-8", response: joinGroupResponseV0{ProtocolName: "\xff"}},
		{name: "protocol name too long", response: joinGroupResponseV0{ProtocolName: strings.Repeat("x", 32768)}},
		{name: "invalid leader UTF-8", response: joinGroupResponseV0{Leader: "\xff"}},
		{name: "leader too long", response: joinGroupResponseV0{Leader: strings.Repeat("x", 32768)}},
		{name: "invalid member id UTF-8", response: joinGroupResponseV0{MemberID: "\xff"}},
		{name: "member id too long", response: joinGroupResponseV0{MemberID: strings.Repeat("x", 32768)}},
		{name: "invalid nested member id UTF-8", response: joinGroupResponseV0{Members: []joinGroupMemberV0{{MemberID: "\xff"}}}},
		{name: "nested member id too long", response: joinGroupResponseV0{Members: []joinGroupMemberV0{{MemberID: strings.Repeat("x", 32768)}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeJoinGroupResponseV0(test.response); err == nil {
				t.Error("encodeJoinGroupResponseV0() error = nil, want encoding error")
			}
		})
	}
}
