package kafka

import "fmt"

type apiVersionsRequest struct {
	clientSoftwareName    string
	clientSoftwareVersion string
}

// decodeAPIVersionsRequestV3 decodes the flexible ApiVersions v3 request body.
func decodeAPIVersionsRequestV3(body []byte) (apiVersionsRequest, error) {
	name, nameBytes, err := decodeCompactString(body)
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 client software name: %w", err)
	}
	version, versionBytes, err := decodeCompactString(body[nameBytes:])
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 client software version: %w", err)
	}
	tagsStart := nameBytes + versionBytes
	tagsBytes, err := skipTaggedFields(body[tagsStart:])
	if err != nil {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 tagged fields: %w", err)
	}
	if tagsStart+tagsBytes != len(body) {
		return apiVersionsRequest{}, fmt.Errorf("decode ApiVersions v3 request: %d trailing bytes", len(body)-tagsStart-tagsBytes)
	}
	return apiVersionsRequest{clientSoftwareName: name, clientSoftwareVersion: version}, nil
}
