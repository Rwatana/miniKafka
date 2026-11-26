# Serving ApiVersions on a Kafka connection

## Kafka concept

Kafka keeps TCP connections open and processes ordered request/response frames
on each connection. Each request begins with a four-byte size and each
response carries the matching correlation ID.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
describes a binary TCP protocol, size-delimited requests, and ordered response
delivery on a connection.

## Go design

`ServeAPIVersionsV3Connection` repeatedly reads bounded request frames, delegates
to the ApiVersions v3 handler, and writes each response fully before reading
the next request. Clean EOF ends the loop; partial frames, protocol errors,
and failed writes are returned.

## Tests

`net.Pipe` fixtures send two framed requests on one connection and verify both
responses in order. Additional cases exercise partial frames, invalid requests,
write errors, and zero-byte writes.

## Deliberate differences

This connection loop currently serves only ApiVersions v3. A TCP listener,
connection management, and the remaining Kafka APIs are separate work.
