# Serving mixed Kafka APIs on one connection

## Kafka concept

Kafka clients reuse a TCP connection for multiple ordered request/response
exchanges. Each request is length-prefixed, and the response must preserve the
correlation ID so the client can match it to the outstanding request.

## Go design

`ServeKafkaConnection` reads one bounded frame at a time, calls
`HandleKafkaRequest`, and writes the complete response before reading the next
frame. It handles partial writes and treats a clean EOF between frames as a
normal connection close.

## Tests

An in-memory TCP pair exchanges ApiVersions v3 and Metadata v0 requests on the
same connection. Other cases cover truncated frames, unsupported APIs, write
errors, zero-byte writes, and partial writes.

## Deliberate limits

The new connection loop is not yet selected by listener startup. Dynamic
metadata refresh and listener wiring are later steps.
