# Encoding Kafka response frames

## Kafka concept

Kafka messages are length-delimited. The four-byte signed size prefix counts
the bytes that follow it, including the response header and API response body.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
specifies the outer network framing as a four-byte message size followed by
the request or response payload, encoded in network byte order.

## Go design

`encodeResponseFrame` writes the body length as a big-endian INT32, copies the
body, and rejects invalid configured limits or bodies larger than that limit.
The caller supplies the maximum frame size so the response encoder can apply
the broker's configured network bound before allocating.

## Tests

Byte fixtures cover a non-empty frame and empty body. Boundary tests reject
non-positive limits and a body above the configured maximum.

## Deliberate differences

This function frames already-encoded response bytes. It does not select a
response header version or encode an API-specific response schema.
