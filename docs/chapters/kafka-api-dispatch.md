# Dispatching Kafka API requests

## Kafka concept

Kafka's common request header begins with an API key and version. A broker
uses the key to select the API-specific schema and handler; each API then
validates the version and header format it supports.

## Go design

`HandleKafkaRequest` reads the fixed-width API key and dispatches Metadata v0
to `HandleMetadataRequestV0` and ApiVersions v3 to
`HandleAPIVersionsRequestV3`. The handlers keep their own version and schema
validation. API keys without an implementation return an explicit error.

## Tests

Tests verify both API routes, response framing, and errors for a truncated
request and an unimplemented API key.

## Deliberate limits

This function routes a single in-memory request frame. The TCP connection
loop still needs to call it, and most Apache Kafka APIs remain unimplemented.
