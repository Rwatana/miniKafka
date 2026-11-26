# Decoding Kafka compact nullable strings

## Kafka concept

Flexible Kafka protocol schemas encode nullable strings as a compact length
followed by UTF-8 bytes. Zero means null; otherwise the stored unsigned value
is the byte length plus one, allowing empty strings to remain distinct from
null.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines `COMPACT_NULLABLE_STRING` in its protocol primitive types. It is used
by flexible request and response schemas.

## Go design

`decodeCompactNullableString` delegates its prefix to
`decodeUnsignedVarint`, checks the remaining frame length before converting
the declared size to `int`, validates UTF-8, and returns both the nullable
string and total consumed bytes. It leaves following fields untouched.

## Tests

Tests cover null, empty, ASCII, and multibyte values, exact consumed byte
counts, trailing bytes, truncated prefixes and bodies, oversized lengths, and
invalid UTF-8.

## Deliberate differences

This primitive handles a single string only. It does not parse flexible header
tag buffers or compact arrays.
