# Handling ApiVersions request v0

## Kafka concept

Kafka clients use ApiVersions to learn which request schemas a broker can
serve. Version 0 has an empty request body and returns an error code followed
by the broker's supported API key/version ranges.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ApiVersions v0 as API key 18 with request header v1. The response uses
header v0 and encodes `api_keys` as a fixed-width INT32-length array of three
INT16 fields.

## Go design

`HandleAPIVersionsRequestV0` validates the shared request header, checks the
API key/version and empty body, then encodes the correlation ID, success code,
and the caller-provided version ranges. The common response-frame encoder
enforces the configured maximum response size.

## Tests

An exact-byte fixture checks the fixed-width array, response header, and
correlation ID. Invalid headers, API keys, versions, unexpected body bytes,
and response-size limits are also covered.

## Deliberate differences

This function answers one ApiVersions v0 request. Dispatching v0 alongside the
existing v3 handler is a separate function-sized change.
