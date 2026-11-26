# Discovering Kafka binary partitions

## Kafka concept

Metadata responses enumerate the topic partitions a broker can serve. Every
partition created through the Kafka protocol must therefore be discoverable
from the broker's native log files.

## Go design

`Broker.ListPartitions` recognizes both the reference JSON `.log` suffix and
the Kafka binary `.kafka.log` suffix, strips each format correctly, and
deduplicates identities when both stores contain the same topic-partition.
Directory order remains deterministic.

## Tests

Tests cover JSON discovery, Kafka-only discovery, a partition duplicated
across both formats, invalid filenames, and directory entries.

## Deliberate limits

The reference JSON and binary Kafka logs remain separate storage formats.
This step only makes Kafka-created partitions visible to Metadata; it does not
replicate data between them.
