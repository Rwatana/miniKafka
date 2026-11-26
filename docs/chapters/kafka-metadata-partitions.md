# Mapping broker partitions to Kafka metadata

## Kafka concept

Metadata describes each topic's partition indexes and its leader, replica, and
in-sync replica broker IDs. A client uses this map to select the broker that
serves the partition.

## Go design

`metadataTopicsFromPartitions` groups `broker.PartitionInfo` values into the
Kafka protocol's `MetadataTopic` and `MetadataPartition` types. It sorts topic
names and partition indexes for deterministic responses and advertises the
single configured broker as leader, replica, and in-sync replica.

## Tests

Tests cover an empty inventory, multiple topics, unsorted partition indexes,
duplicate inventory entries, and the broker ID in every replica field.

## Deliberate limits

The current storage model has no replica placement or leader election. This
mapping therefore reports the existing single broker only; it does not model
Apache Kafka's distributed metadata controller or replication protocol.
