# Decoding Kafka unsigned VARLONG values

## Kafka concept

Kafka VARLONG fields use a variable-length integer that can span the full
64-bit range. Signed VARLONG values apply zig-zag mapping on top of this
unsigned base-128 representation.

## Go design

`decodeUnsignedVarlong` reads at most ten bytes, checks that the tenth byte
fits within 64 bits, and returns the number of bytes consumed. The signed
zig-zag conversion will build on this primitive in a later step.

## Tests

Fixtures cover zero, multi-byte values, uint64 maximum, following bytes,
truncation, a tenth-byte overflow, and an unterminated tenth byte.

## Deliberate limits

This parser handles only the unsigned representation; Kafka signed VARLONG
decoding is a separate function-sized change.
