# Encoding Kafka response header v0

## Kafka concept

Kafka responses include the request's correlation ID so clients can match
responses with the requests they sent on a TCP connection.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Response Header v0 as a single INT32 correlation ID encoded in network
byte order.

## Go design

`encodeResponseHeaderV0` returns exactly four big-endian bytes. It does not add
the outer message-size prefix or any API-specific response body; those are
separate protocol layers.

## Tests

Byte fixtures verify zero, positive, and negative correlation IDs and assert
the exact header length.

## Deliberate differences

This function implements the fixed response header only. Flexible response
header v1 and the outer response frame are separate functions.
