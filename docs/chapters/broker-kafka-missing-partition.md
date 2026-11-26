# Distinguishing missing Kafka partitions

## Kafka concept

An empty partition and an unknown topic-partition are different protocol
states. A real empty partition has a log and a high watermark of zero; a
missing partition must produce `UNKNOWN_TOPIC_OR_PARTITION` in Fetch.

## Go design

`Broker.ReadKafkaRecordBatches` checks whether the binary partition log exists
before opening it. Missing files return the shared
`kafkaerr.ErrUnknownTopicOrPartition` sentinel with topic and partition
context. Existing empty files are opened and read as successful empty logs.

## Tests

Tests distinguish empty existing files from missing files, ensure a missing
read does not create storage, and verify callers can match the typed error with
`errors.Is`.

## Deliberate limits

The Fetch handler still needs to translate the shared sentinel into Kafka's
numeric error code. Topic auto-creation is not supported.
