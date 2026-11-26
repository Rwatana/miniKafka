# Handling ListOffsets request v1

## Kafka concept

ListOffsets resolves requested partition timestamps before Fetch. Kafka sends
earliest/latest sentinels as -2/-1 and expects one partition result per
request, including protocol error codes for failed lookups.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ListOffsets key 2 version 1 with request header v1 and response header
v0. Its v1 response includes timestamp and offset for each partition.

## Go design

`HandleListOffsetsRequestV1` validates the request and delegates each lookup
to `KafkaOffsetLookup`. Successful values pass through unchanged. Unknown
topic/partition becomes error 3; other storage failures become error -1.
Results retain request order and use the common bounded frame encoder.

## Tests

Tests verify sentinel forwarding, response correlation and ordering, success,
unknown-partition and server errors, malformed frames, missing callbacks, and
size bounds.

## Deliberate differences

The handler delegates offset semantics to storage. The broker lookup must
implement earliest/latest/timestamp matching, and API dispatch remains a
separate change.
