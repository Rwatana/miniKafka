package kafka

import "testing"

func TestDecodeAPIVersionsRequestV3(t *testing.T) {
	tests := []struct {
		name    string
		body    []byte
		want    apiVersionsRequest
		wantErr bool
	}{
		{name: "client fields and empty tags", body: []byte{5, 'j', 'a', 'v', 'a', 5, '1', '.', '2', '0', 0}, want: apiVersionsRequest{clientSoftwareName: "java", clientSoftwareVersion: "1.20"}},
		{name: "empty strings and unknown tagged field", body: []byte{1, 1, 1, 0, 1, 0xaa}, want: apiVersionsRequest{}},
		{name: "malformed first string", body: []byte{0x80}, wantErr: true},
		{name: "truncated first string", body: []byte{2, 'x'}, wantErr: true},
		{name: "truncated second string", body: []byte{2, 'x', 2}, wantErr: true},
		{name: "missing tagged fields", body: []byte{2, 'x', 2, 'v'}, wantErr: true},
		{name: "malformed tagged fields", body: []byte{2, 'x', 2, 'v', 1}, wantErr: true},
		{name: "trailing bytes", body: []byte{2, 'x', 2, 'v', 0, 0xff}, wantErr: true},
		{name: "duplicate tags", body: []byte{2, 'x', 2, 'v', 2, 1, 0, 1, 0}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeAPIVersionsRequestV3(test.body)
			if (err != nil) != test.wantErr {
				t.Fatalf("decodeAPIVersionsRequestV3() error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && got != test.want {
				t.Errorf("request = %+v, want %+v", got, test.want)
			}
		})
	}
}
