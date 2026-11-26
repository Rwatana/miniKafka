# Resolving Kafka partition offsets

## Kafka concept

ListOffsets supports earliest/latest position queries and timestamp searches.
The protocol uses -2 for earliest, -1 for latest, and non-negative timestamps
to find the first record whose timestamp is at least the requested value.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ListOffsets v1 partition timestamps and the special earliest/latest
values. Its response reports both the matched timestamp and offset.

## Go design

`Broker.LookupKafkaOffset` reads the broker's Kafka batches, returns the first
record position for -2, the next partition offset and last batch timestamp for
-1, and scans uncompressed magic-v2 records for timestamp queries. Timestamp
matching rejects compressed batches until a matching codec decoder is added.

## Tests

Tests cover earliest/latest sentinels, first record timestamp match, no match,
empty and missing partitions, invalid timestamp values, and unsupported
compression.

## Deliberate differences

Compressed record timestamp searches are not supported yet. Record timestamps
and offset bounds are currently read by scanning batches rather than using an
on-disk timestamp index.
