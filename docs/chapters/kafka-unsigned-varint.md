# Decoding Kafka unsigned varints

## Kafka concept

Flexible Kafka protocol versions encode compact lengths with an unsigned
base-128 varint. This differs from signed `VARINT`, which uses zig-zag
encoding. A compact string stores its byte length plus one, reserving zero for
null in the nullable form.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines `UNSIGNED_VARINT` as the length prefix for compact strings, byte
arrays, and arrays, and defines `VARINT` separately as zig-zag encoded.

## Go design

`decodeUnsignedVarint` reads at most five bytes into a `uint32`, returns the
number of bytes consumed so callers can continue parsing the same frame, and
rejects truncation, overflow, or a continuation bit after the fifth byte.
Signed zig-zag decoding remains a separate primitive.

## Tests

Fixtures cover one- through five-byte values, the `uint32` maximum, trailing
bytes, empty and truncated input, too many continuation bytes, and overflow.

## Deliberate differences

This helper decodes only the unsigned encoding used for compact field lengths.
It does not interpret arrays, strings, tagged fields, or record varints.
