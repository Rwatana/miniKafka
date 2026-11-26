# Decoding ListOffsets request v1

## Kafka concept

Consumers ask ListOffsets for the earliest, latest, or timestamp-matched
offset of a partition before reading records. The request is grouped by topic
and carries a timestamp for each partition.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ListOffsets API key 2, v1 as a replica ID followed by topics containing
partition indexes and INT64 timestamps. A normal consumer uses replica ID -1;
timestamps -2 and -1 identify latest and earliest offsets.

## Go design

`decodeListOffsetsRequestV1` parses the fixed-width arrays in order, preserves
the signed replica ID and timestamp values, validates UTF-8 names and counts,
rejects negative partition indexes, and requires the body to end exactly
after the final partition.

## Tests

Fixtures cover empty and populated requests, latest/earliest timestamp
sentinels, malformed counts and strings, truncated nested arrays, negative
indexes, and trailing bytes.

## Deliberate differences

This function parses only ListOffsets v1 request bodies. Offset lookup, the
response schema, request routing, and startup advertisement are separate work.
