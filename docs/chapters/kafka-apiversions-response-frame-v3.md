# Framing an ApiVersions response v3

## Kafka concept

Kafka sends a response correlation ID and API body inside an outer message-size
frame. The size counts every byte after the prefix, including the response
header.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines a four-byte INT32 message size and a v0 response header containing the
correlation ID. ApiVersions v3 uses that response header with its flexible body.

## Go design

`encodeAPIVersionsResponseFrameV3` composes the v0 header, the v3 API body, and
the bounded response-frame encoder. The caller supplies the correlation ID,
API ranges, and configured maximum frame size.

## Tests

An exact-byte fixture checks the outer size, correlation ID, and one supported
API range. A boundary case verifies that an undersized configured limit is
rejected.

## Deliberate differences

This composes the wire response but does not read sockets, dispatch requests,
or advertise APIs beyond the ranges supplied by its caller.
