# Recovering Kafka partition offsets

## Kafka concept

Kafka reconstructs partition state from its append-only record-batch log when a
broker starts. The next offset follows the final batch's base offset and last
offset delta, so a restart does not reuse offsets already written.

## Go design

`RecoverKafkaPartitionLog` scans each fixed batch header and length, verifies
magic-v2 and CRC32C, validates offset fields, and records the next offset after
the final batch. It uses bounded reads so a large batch need not be loaded into
memory at once.

## Tests

Fixtures cover empty, single, and multiple batches, plus truncation, bad magic,
bad CRC, invalid counts or offsets, decreasing offsets, overflow, and closed
handles.

## Deliberate limits

Recovery validates batch framing and checksums. Full record decoding and broker
integration remain separate steps.
