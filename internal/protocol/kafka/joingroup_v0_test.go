package kafka

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDecodeJoinGroupRequestV0(t *testing.T) {
	body := appendJoinGroupStringV0ForTest(nil, "workers")
	body = binary.BigEndian.AppendUint32(body, 30000)
	body = appendJoinGroupStringV0ForTest(body, "member-1")
	body = appendJoinGroupStringV0ForTest(body, "consumer")
	body = binary.BigEndian.AppendUint32(body, 1)
	body = appendJoinGroupStringV0ForTest(body, "range")
	body = binary.BigEndian.AppendUint32(body, 3)
	body = append(body, 1, 2, 3)

	got, err := decodeJoinGroupRequestV0(body)
	if err != nil {
		t.Fatalf("decodeJoinGroupRequestV0() error = %v", err)
	}
	want := joinGroupRequestV0{
		GroupID:        "workers",
		SessionTimeout: 30000,
		MemberID:       "member-1",
		ProtocolType:   "consumer",
		Protocols:      []joinGroupProtocolV0{{Name: "range", Metadata: []byte{1, 2, 3}}},
	}
	if got.GroupID != want.GroupID || got.SessionTimeout != want.SessionTimeout || got.MemberID != want.MemberID || got.ProtocolType != want.ProtocolType || len(got.Protocols) != 1 || got.Protocols[0].Name != want.Protocols[0].Name || !bytes.Equal(got.Protocols[0].Metadata, want.Protocols[0].Metadata) {
		t.Errorf("decodeJoinGroupRequestV0() = %+v, want %+v", got, want)
	}
}

func TestDecodeJoinGroupRequestV0RejectsInvalidBodies(t *testing.T) {
	valid := appendJoinGroupStringV0ForTest(nil, "g")
	valid = binary.BigEndian.AppendUint32(valid, 1000)
	valid = appendJoinGroupStringV0ForTest(valid, "m")
	valid = appendJoinGroupStringV0ForTest(valid, "consumer")
	valid = binary.BigEndian.AppendUint32(valid, 1)
	valid = appendJoinGroupStringV0ForTest(valid, "range")
	valid = binary.BigEndian.AppendUint32(valid, 1)
	valid = append(valid, 42)

	missingTimeout := appendJoinGroupStringV0ForTest(nil, "g")
	negativeTimeout := appendJoinGroupStringV0ForTest(nil, "g")
	negativeTimeout = binary.BigEndian.AppendUint32(negativeTimeout, ^uint32(0))
	missingMember := appendJoinGroupStringV0ForTest(nil, "g")
	missingMember = binary.BigEndian.AppendUint32(missingMember, 1000)
	missingProtocolType := appendJoinGroupStringV0ForTest(nil, "g")
	missingProtocolType = binary.BigEndian.AppendUint32(missingProtocolType, 1000)
	missingProtocolCount := appendJoinGroupStringV0ForTest(nil, "g")
	missingProtocolCount = binary.BigEndian.AppendUint32(missingProtocolCount, 1000)
	missingProtocolCount = appendJoinGroupStringV0ForTest(missingProtocolCount, "m")
	missingProtocolCount = appendJoinGroupStringV0ForTest(missingProtocolCount, "consumer")
	negativeProtocolCount := append(append([]byte(nil), missingProtocolCount...), 0xff, 0xff, 0xff, 0xff)
	impossibleProtocolCount := append(append([]byte(nil), missingProtocolCount...), 0, 0, 0, 1)
	missingMetadataLength := appendJoinGroupStringV0ForTest(nil, "g")
	missingMetadataLength = binary.BigEndian.AppendUint32(missingMetadataLength, 1000)
	missingMetadataLength = appendJoinGroupStringV0ForTest(missingMetadataLength, "m")
	missingMetadataLength = appendJoinGroupStringV0ForTest(missingMetadataLength, "consumer")
	missingMetadataLength = binary.BigEndian.AppendUint32(missingMetadataLength, 1)
	missingMetadataLength = appendJoinGroupStringV0ForTest(missingMetadataLength, "range")
	negativeMetadataLength := append(append([]byte(nil), missingMetadataLength...), 0xff, 0xff, 0xff, 0xff)
	truncatedMetadata := append(append([]byte(nil), missingMetadataLength...), 0, 0, 0, 2, 1)
	trailing := append(append([]byte(nil), valid...), 0)
	invalidGroupUTF8 := []byte{0, 1, 0xff}
	invalidMemberUTF8 := appendJoinGroupStringV0ForTest(nil, "g")
	invalidMemberUTF8 = binary.BigEndian.AppendUint32(invalidMemberUTF8, 1)
	invalidMemberUTF8 = append(invalidMemberUTF8, 0, 1, 0xff)
	invalidProtocolTypeUTF8 := appendJoinGroupStringV0ForTest(nil, "g")
	invalidProtocolTypeUTF8 = binary.BigEndian.AppendUint32(invalidProtocolTypeUTF8, 1)
	invalidProtocolTypeUTF8 = appendJoinGroupStringV0ForTest(invalidProtocolTypeUTF8, "m")
	invalidProtocolTypeUTF8 = append(invalidProtocolTypeUTF8, 0, 1, 0xff)
	invalidProtocolNameUTF8 := appendJoinGroupStringV0ForTest(nil, "g")
	invalidProtocolNameUTF8 = binary.BigEndian.AppendUint32(invalidProtocolNameUTF8, 1)
	invalidProtocolNameUTF8 = appendJoinGroupStringV0ForTest(invalidProtocolNameUTF8, "m")
	invalidProtocolNameUTF8 = appendJoinGroupStringV0ForTest(invalidProtocolNameUTF8, "consumer")
	invalidProtocolNameUTF8 = binary.BigEndian.AppendUint32(invalidProtocolNameUTF8, 1)
	invalidProtocolNameUTF8 = append(invalidProtocolNameUTF8, 0, 1, 0xff)
	negativeGroupLength := []byte{0xff, 0xff}
	oversizedGroupLength := []byte{0, 2, 'g'}

	for _, test := range []struct {
		name string
		body []byte
	}{
		{name: "missing group length", body: nil},
		{name: "negative group length", body: negativeGroupLength},
		{name: "group exceeds body", body: oversizedGroupLength},
		{name: "invalid group UTF-8", body: invalidGroupUTF8},
		{name: "missing timeout", body: missingTimeout},
		{name: "negative timeout", body: negativeTimeout},
		{name: "missing member", body: missingMember},
		{name: "invalid member UTF-8", body: invalidMemberUTF8},
		{name: "missing protocol type", body: missingProtocolType},
		{name: "invalid protocol type UTF-8", body: invalidProtocolTypeUTF8},
		{name: "missing protocol count", body: missingProtocolCount},
		{name: "negative protocol count", body: negativeProtocolCount},
		{name: "impossible protocol count", body: impossibleProtocolCount},
		{name: "invalid protocol name UTF-8", body: invalidProtocolNameUTF8},
		{name: "missing metadata length", body: missingMetadataLength},
		{name: "negative metadata length", body: negativeMetadataLength},
		{name: "metadata exceeds body", body: truncatedMetadata},
		{name: "trailing bytes", body: trailing},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeJoinGroupRequestV0(test.body); err == nil {
				t.Error("decodeJoinGroupRequestV0() error = nil, want decode error")
			}
		})
	}
}

func appendJoinGroupStringV0ForTest(body []byte, value string) []byte {
	body = binary.BigEndian.AppendUint16(body, uint16(len(value)))
	return append(body, value...)
}
