# Decoding Kafka request header v2

## Kafka concept

Flexible Kafka APIs use request header v2. The fixed API key, API version, and
correlation ID are followed by a compact nullable client ID and a tagged-field
buffer before the API-specific body.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
maps flexible API versions to request header v2 and defines compact strings
and tagged-field sections for schema evolution.

## Go design

`DecodeRequestHeaderV2` parses the fixed-width values in network byte order,
delegates the client ID to `decodeCompactNullableString`, skips the header tags
with `skipTaggedFields`, and returns the untouched API body. Header selection
by API key and version remains a separate dispatch step.

## Tests

Tests cover a flexible header with an unknown tag, preserved body bytes, null
and empty client IDs, a truncated fixed header, missing client ID/tag buffer,
and malformed tag ordering.

## Deliberate differences

This function decodes header v2 only. It does not parse the API-specific body
or choose a header version based on the API version table.
