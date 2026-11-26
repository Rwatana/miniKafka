# Decoding Kafka signed VARLONG values

## Kafka concept

Record timestamps in Kafka's magic-v2 format are signed millisecond deltas.
The value uses zig-zag mapping and a variable-length 64-bit representation so
small positive and negative deltas take fewer bytes.

## Go design

`decodeSignedVarlong` delegates byte-boundary and overflow checks to
`decodeUnsignedVarlong`, applies the int64 zig-zag transform, and returns the
consumed length for sequential record decoding.

## Tests

Fixtures cover both int64 boundaries, values around zero, multi-byte
boundaries, following bytes, truncation, and final-byte overflow.

## Deliberate limits

This is only the integer primitive. Timestamp accumulation and record parsing
are handled by later steps.
