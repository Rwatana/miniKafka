# Handling Kafka Produce v3 requests

## Kafka concept

Produce v3 carries topic-partition record sets and an acknowledgement mode. The
broker validates record batches, appends them to the selected partition logs,
and returns offsets and errors per partition unless the client requested no
response with `acks=0`.

## Go design

`HandleProduceRequestV3` validates the request header and body, checks each
magic-v2 batch and uncompressed record sequence before any append, calls a
broker-supplied appender, and encodes the partition outcomes in a correlation
matched response frame.

## Tests

Fixtures cover multiple batches and partitions, acks=0, invalid batches that
must not be appended, append failures, invalid headers, transactions, and frame
size limits.

## Deliberate limits

Transactional Produce and compressed record batches are unsupported. The
dispatcher and listener are not wired to this handler yet.
