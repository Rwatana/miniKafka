# Decoding Kafka signed VARINT values

## Kafka concept

Kafka's compact record format uses signed variable-length integers for record
lengths, offsets, timestamps, and nullable field lengths. This signed encoding
uses zig-zag mapping before the underlying unsigned base-128 representation.

## Go design

`decodeSignedVarint` delegates the bounded byte parsing to
`decodeUnsignedVarint`, then converts the unsigned value to signed int32 using
zig-zag decoding. It returns the consumed byte count so callers can continue
reading the next record field.

## Tests

Fixtures cover both int32 limits, values around zero and varint-size
boundaries, bytes following a decoded value, truncation, and overflow.

## Deliberate limits

Kafka signed VARLONG values require up to ten bytes and will be implemented as
a separate parsing step.
