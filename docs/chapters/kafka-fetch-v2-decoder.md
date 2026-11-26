# Decoding Fetch v2

## Kafka concept

Fetch request versions evolve their fields over time. Fetch v2 has the shared
replica ID, wait time, minimum bytes, topic/partition lists, and per-partition
maximum bytes, while Fetch v4 adds an aggregate maximum and isolation level.

## Go design

`decodeFetchRequestV2` parses the v2 field order into the existing topic and
partition request shapes. It validates signed limits, string encoding, array
bounds, partition offsets, and trailing bytes before a handler can read logs.

## Tests

Tests cover a populated request, an empty request, and malformed sizes, names,
counts, partition values, and trailing data.

## Deliberate limits

This step only decodes the request body. Response encoding, handler dispatch,
and listener compatibility are separate changes.
