# Dispatching Kafka API requests

## Kafka concept

Kafka's common request header begins with an API key and version. A broker
uses the key to select the API-specific schema and handler; each API then
validates the version and header format it supports.

## Go design

`HandleKafkaRequest` reads the fixed-width API key and dispatches Metadata v0
and v1 to `HandleMetadataRequestV0` and `HandleMetadataRequestV1`, respectively.
It dispatches ApiVersions v0 to
`HandleAPIVersionsRequestV0`, and ApiVersions v3 to
`HandleAPIVersionsRequestV3`. ApiVersions also needs the schema version from
the request header to select its decoder. The handlers keep their own version
and body validation. API keys and versions without an implementation return
an explicit error.

`HandleKafkaRequestWithFetchReader` is the reader-aware entry point for the
Fetch v4 handler. It delegates other API keys to the original dispatcher so
existing Produce, Metadata, and ApiVersions callers retain their current
contract while Fetch is threaded through the connection stack in later steps.

## Tests

Tests verify the existing API routes, Fetch delegation, response framing, and
errors for a truncated request and an unimplemented API key.

## Deliberate limits

This function routes a single in-memory request frame. The TCP connection
loop still needs to call it, and most Apache Kafka APIs remain unimplemented.
