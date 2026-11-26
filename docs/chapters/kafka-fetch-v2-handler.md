# Handling Fetch v2

## Kafka concept

Fetch v2 reads the requested offset for each partition, returns the current
high watermark, and observes the request's per-partition byte limit. A request
may wait for `min_bytes`, but it must stop waiting at `max_wait_ms`.

## Go design

`HandleFetchRequestV2` validates the request header and body, calls the broker
batch reader, maps storage errors to Kafka partition errors, and encodes a v2
response with the matching correlation ID.

## Tests

Tests cover callback arguments, successful records, missing partitions, storage
errors, offsets beyond the high watermark, minimum-byte waiting, malformed
frames, and response limits.

## Deliberate limits

This function is not yet reachable from the API dispatcher. v2 has no aggregate
request `max_bytes` field; it honors each partition's individual limit.
