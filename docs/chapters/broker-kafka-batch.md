# Appending Kafka batches through the broker

## Kafka concept

The broker owns partition logs and serializes appends per broker. It must open
existing partition state and recover its offset before accepting another batch
after a restart.

## Go design

`Broker.AppendKafkaRecordBatch` validates the topic and partition, holds the
broker mutex, caches a binary Kafka log handle, recovers the handle on first
open, and delegates the durable batch append to storage.

## Tests

Fixtures cover cache reuse, restart recovery, concurrent appends, invalid
identifiers, and storage open, recovery, and append failures.

## Deliberate limits

The broker method accepts an already validated record batch. Kafka Produce
request decoding and response handling will be wired in a later step.
