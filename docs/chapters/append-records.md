# Appending a batch of partition records

## Kafka concept

Kafka writes events to an append-only partition log in order. Grouping several
records into one write reduces per-write overhead while preserving one offset
per record. The [ByteByteGo Kafka overview](https://blog.bytebytego.com/p/why-is-kafka-so-fast-how-does-it)
describes the sequential append-only data path; this step adds a small batching
experiment on top of the reference MiniKafka behavior.

## Reference source

The source MiniKafka exposes one-record
`append` in [`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js).
Batch append is a Go extension and does not change that reference API.

## Go design

`AppendRecords` assigns each input the next contiguous offset and a Unix
millisecond timestamp, encodes the complete batch before touching the file, and
issues one file write. It records byte positions only after the full write
succeeds. A short or failed write truncates the partial batch back to its
starting byte position. An empty batch performs no write.

## Tests

Filesystem tests verify one write per batch, keyed and null-key round trips,
sequential offsets, current timestamps, empty input, encode failure, and
short-write rollback.

## Deliberate differences

The original MiniKafka accepts one record per call. This batch primitive is a
Kafka-inspired performance extension, not a claim of Apache Kafka protocol or
storage compatibility. Producer request batching and segment files remain
separate steps.
