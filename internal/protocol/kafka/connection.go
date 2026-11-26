package kafka

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// ServeAPIVersionsV3Connection serves ordered ApiVersions v3 frames until the connection closes.
func ServeAPIVersionsV3Connection(conn net.Conn, maxFrameBytes int32, supportedAPIs []APIVersionRange) error {
	for {
		requestFrame, err := ReadRequestFrame(conn, maxFrameBytes)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read Kafka ApiVersions request: %w", err)
		}
		response, err := HandleAPIVersionsRequestV3(requestFrame, maxFrameBytes, supportedAPIs)
		if err != nil {
			return fmt.Errorf("handle Kafka ApiVersions request: %w", err)
		}
		for len(response) > 0 {
			written, err := conn.Write(response)
			if err != nil {
				return fmt.Errorf("write Kafka ApiVersions response: %w", err)
			}
			if written == 0 {
				return io.ErrShortWrite
			}
			response = response[written:]
		}
	}
}
