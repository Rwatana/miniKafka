# Appending a partition record

## Kafka concept

Kafka's write path is sequential: a producer appends a record to the end of one
partition log and receives that record's offset. The in-memory index is updated
so a later consumer can seek directly to the new record.

## Reference source

`append` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
uses the current number of positions as the next offset, writes a JSON line,
then adds the file's prior size to the index.

## Go design

`appendRecord` reads the next offset from the position count, encodes the
reference fields, seeks to the end of the append-mode file, writes one line,
then adds its starting byte position. It generates the reference's numeric
Unix millisecond timestamp with `time.Now().UnixMilli()`. Failed writes do not
advance the in-memory index. Batching, checksums, and explicit fsync policy are
not part of this function.

## Tests

Filesystem tests append keyed and null-key values, verify sequential offsets,
check line termination, reopen the log to recover positions, and exercise nil,
seek, encoding, and write failures.

## Deliberate differences

The reference caches write streams and updates a shared positions array. This
Go version keeps state on one `partitionLog` value. Synchronizing concurrent
producers and durable flush policy will be separate behaviors.
