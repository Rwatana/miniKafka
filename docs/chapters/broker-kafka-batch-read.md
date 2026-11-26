# Reading Kafka batches through the broker

## Kafka concept

Fetch asks for records at an offset and includes a maximum byte budget. Kafka
returns complete record batches, rather than cutting a batch in the middle, and
reports the partition high watermark so a consumer can tell which offsets are
replicated and visible.

## Go design

`Broker.ReadKafkaRecordBatches` serializes reads with appends, validates the
partition request, and delegates batch boundaries and high-watermark scanning
to the storage log. If the broker has not opened a log yet, it opens and
recovers an existing file, then caches the handle for `Broker.Close`. A
partition with no file reads as empty and does not create storage as a side
effect.

## Tests

Tests cover cached reads, recovery after restart, empty missing partitions,
invalid offsets and limits, filesystem and recovery failures, and storage read
errors.

## Deliberate limits

This broker method is not a Kafka Fetch API handler. Wire protocol validation,
isolation behavior, request limits, and TCP dispatch are separate steps.
