# Handling Metadata requests v0

## Kafka concept

Clients ask a broker for metadata before choosing a broker and partition for
Produce or Fetch. The broker returns the requested topics' partitions and
identifies unknown topics with a per-topic error.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Metadata API key 3, v0 requests with a legacy topic array, and a
response header containing the request correlation ID. For v0, an empty topic
array requests metadata for all available topics. Unknown topic names use
`UNKNOWN_TOPIC_OR_PARTITION` (error code 3).

## Go design

`HandleMetadataRequestV0` validates the request header and API version, decodes
the requested names, selects the supplied metadata, encodes the v0 response,
and applies the bounded response frame. It receives broker state as arguments;
discovering that state from MiniKafka's partition logs remains a separate
integration step.

## Tests

Fixtures verify known and unknown topic responses, the empty-array all-topics
case, correlation ID preservation, and malformed headers, unsupported API
versions, malformed bodies, invalid metadata strings, and response size limits.

## Deliberate differences

This is one protocol handler over caller-supplied metadata. It does not yet
discover partitions from storage or dispatch requests from the TCP listener.
