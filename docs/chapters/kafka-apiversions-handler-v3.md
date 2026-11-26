# Handling ApiVersions request v3

## Kafka concept

ApiVersions starts Kafka client compatibility negotiation. A broker must
decode the request header and request schema, then send a response that carries
the same correlation ID so the client can match the reply.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ApiVersions as API key 18, v3 with request header v2, and a response
header v0 carrying the request correlation ID.

## Go design

`HandleAPIVersionsRequestV3` validates the flexible header, API key/version,
and complete v3 body. It then returns the success response using caller
supplied API version ranges and frame bounds.

## Tests

An exact-byte request/response fixture checks the correlation ID and full
response framing. Invalid API key/version, malformed body, and an insufficient
response bound are rejected.

## Deliberate differences

This handles one ApiVersions v3 frame only. It does not accept TCP connections,
process other Kafka APIs, or maintain broker state.
