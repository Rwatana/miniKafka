# Decoding ApiVersions request v3

## Kafka concept

ApiVersions is the first request many Kafka clients use to discover which API
versions a broker supports. Version 3 adds flexible request headers and client
software name/version fields, followed by a tagged-fields section.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ApiVersions API key 18, request v3 with two COMPACT_STRING fields, and
request header version 2. Flexible request components end with tagged fields.

## Go design

`decodeAPIVersionsRequestV3` parses the client identity strings, validates and
skips unknown tagged fields, and rejects any bytes after the tags. This gives
the later handshake handler the client's advertised identity without
interpreting extension tags it does not know.

## Tests

Fixtures cover normal and empty identities, an unknown tagged field, truncated
fields, malformed tags, duplicate tags, and trailing bytes.

## Deliberate differences

This function parses only request schema v3. ApiVersions v4 and v5 are separate
schemas, and the broker response/connection loop are not implemented here.
