# Framing Produce responses

## Kafka concept

Kafka responses use a four-byte size prefix. The response header carries the
request's correlation ID before the API-specific body, allowing a client to
match each response to its request on a persistent connection.

## Go design

`encodeProduceResponseFrameV3` combines the v0 correlation-ID header and the
Produce v3 body, then delegates size-prefixing and maximum-size enforcement to
the shared frame encoder.

## Tests

Fixtures verify the frame length, correlation ID, response body, oversized
frame rejection, and propagation of a response-body encoding error.

## Deliberate limits

The frame builder does not dispatch requests or append records to a partition.
