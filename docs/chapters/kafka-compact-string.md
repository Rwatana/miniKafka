# Decoding Kafka compact strings

## Kafka concept

Flexible Kafka request schemas use compact strings whose length is an unsigned
varint containing the UTF-8 byte length plus one. Unlike compact nullable
strings, a compact string cannot use zero to represent null.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines COMPACT_STRING for flexible schemas. For example, ApiVersions requests
from version 3 use required compact strings for client software name/version.

## Go design

`decodeCompactString` delegates length, bounds, and UTF-8 validation to the
existing nullable decoder, then rejects its null sentinel and returns the
number of bytes consumed. This lets a caller continue with the next schema
field without copying or losing trailing bytes.

## Tests

Fixtures cover an empty value, multibyte UTF-8 followed by another field, null,
malformed varint, and invalid UTF-8.

## Deliberate differences

This function decodes one string field only. It does not parse a particular
Kafka API body or enforce API-specific version rules.
