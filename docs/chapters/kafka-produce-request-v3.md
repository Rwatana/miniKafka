# Decoding Produce requests v3

## Kafka concept

Kafka Produce requests batch records by topic and partition. The request tells
the broker the acknowledgement policy and timeout, then carries a record
batch for each target partition.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Produce API key 0 version 3 with a nullable transactional ID, INT16
acknowledgements, INT32 timeout, topic and partition arrays, and nullable
record bytes. Version 3 uses request header v1. The protocol primitive
`NULLABLE_RECORDS` uses an INT32 byte length, with -1 representing null.

## Go design

`decodeProduceRequestV3` parses the request body, validates counts, UTF-8, and
length bounds, and keeps each partition's record batch opaque. Record batch
magic, CRC, compression, timestamps, and records are parsed by later steps.

## Tests

Fixtures cover multiple topics and partitions, null transactional IDs and
record sets, record bytes, invalid acknowledgements, malformed lengths,
negative or excessive counts, invalid UTF-8, truncation, and trailing bytes.

## Deliberate limits

This function only decodes the Produce v3 body. It does not validate the request
header, decode record batches, append records, or build a Produce response.
