# Decoding Fetch v3

## Kafka concept

Fetch v3 adds a request-wide `max_bytes` limit to the v2 request schema. The
broker still receives per-partition offsets and byte limits, but it also sees
the caller's total response budget.

## Go design

`decodeFetchRequestV3` reads the v3 fixed fields and then the shared topic and
partition arrays. It rejects negative limits, invalid strings, impossible
array sizes, negative offsets, and extra bytes.

## Tests

Tests cover populated and empty requests plus malformed fixed values, names,
array counts, partition fields, and trailing data.

## Deliberate limits

This step only decodes the body. The handler must enforce the aggregate budget
before Fetch v3 can be advertised.
