# Decoding Metadata request v0

## Kafka concept

Metadata is how Kafka clients discover broker and topic partition placement
after ApiVersions negotiation. Older request schemas use fixed-width arrays
and UTF-8 strings rather than the flexible compact encodings.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Metadata API key 3 request v0 as a topic array. Each topic name is a
legacy STRING with an INT16 byte length, and the request uses header version 1.

## Go design

`decodeMetadataRequestV0` reads the INT32 array count, validates that the
remaining bytes can contain at least each STRING length, decodes UTF-8 names,
and rejects invalid lengths or bytes after the array.

## Tests

Fixtures cover an empty array, multiple ASCII/UTF-8 names, malformed counts,
negative lengths, truncated strings, invalid UTF-8, and trailing bytes.

## Deliberate differences

This function parses only Metadata v0's request body. Broker metadata state,
the v0 response, and connection dispatch are separate functions and Issues.
