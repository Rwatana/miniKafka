# Handling Metadata request v1

## Kafka concept

Metadata v1 lets clients request a named topic or use a null topic array to
discover every topic. Its response carries a controller ID, broker rack
fields, and topic internal flags in addition to v0 partition placement.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines API key 3, request version 1 with request header v1 and response
version 1 with response header v0.

## Go design

`HandleMetadataRequestV1` validates the header and version, decodes the
nullable topic list, selects requested topics (or all topics for a null/empty
list), and builds the response. In this single-broker implementation, the
first broker is the controller; with no broker, the controller ID is -1.

## Tests

Tests cover an all-topics request, a specific-topic request, correlation ID,
invalid requests, response encoding errors, and frame bounds. The separate
encoder tests assert every v1 response field byte-for-byte.

## Deliberate differences

This handler does not create topics and does not implement Metadata versions
above 1. Dispatch and API-version advertisement remain separate changes.
