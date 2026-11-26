# Decoding Kafka Fetch v4 requests

## Kafka concept

Fetch requests identify the replica, byte and wait limits, isolation level,
topics, partition indexes, and the first offset the client wants to read.
Fetch v4 uses the non-flexible STRING and ARRAY encodings.

## Go design

`decodeFetchRequestV4` reads the fixed fields, validates limits and isolation
level, then walks length-prefixed topic and partition arrays without consuming
bytes beyond the request body.

## Tests

Fixtures cover multiple topics and partitions, empty topic lists, invalid
limits, malformed counts, invalid UTF-8, negative partition fields, and
trailing bytes.

## Deliberate limits

This step only decodes Fetch requests. Reading binary logs, response encoding,
and connection dispatch are separate implementation steps.
