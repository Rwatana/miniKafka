# Handling Kafka Fetch v4

## Kafka concept

Fetch is the consumer side of the record path. The request identifies topic
partitions, the first offset the client needs, and a per-partition byte limit.
The broker returns record batches plus the partition's high watermark.

## Go design

`HandleFetchRequestV4` validates the non-flexible Kafka request header, decodes
the Fetch v4 body, asks a broker-supplied reader for each requested partition,
and returns a response frame with the request's correlation ID. A partition
read error is represented by Kafka's unknown-server error code while other
partitions remain independently answerable.

The broker's typed unknown-topic-or-partition error maps to Fetch error code 3
with unavailable watermarks and a null record set. Other storage failures keep
the unknown-server error code.

An offset above the returned high watermark produces Kafka's
`OFFSET_OUT_OF_RANGE` error and omits records. An offset equal to the high
watermark remains a valid empty read.

Partition reads share the request's aggregate `max_bytes` budget, bounded by
each partition's `partition_max_bytes`. Kafka's soft-limit behavior is kept:
the first non-empty whole batch can exceed the aggregate byte budget, and
later partitions return metadata without adding more record bytes.

This implementation reports the high watermark as the last stable offset and
uses no aborted-transaction entries because the current broker has no
transaction protocol.

## Tests

Tests verify reader arguments, multiple topic partitions, batch bytes,
correlation matching, high watermarks, per-partition failures, malformed
headers and bodies, a missing reader, and response-size errors.

## Deliberate limits

Fetch v4 long polling, global response byte budgeting, transaction isolation,
unknown-topic metadata errors, and request dispatch are not implemented by this
handler. They remain separate interoperability steps.
