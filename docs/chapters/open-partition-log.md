# Opening and recovering a partition log

## Kafka concept

Each topic partition owns an append-only file. When a broker first opens a
partition after startup, it scans existing lines once to rebuild the offset to
byte-position index, then keeps the file open for later writes.

## Reference source

`loadIndex` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
opens an existing partition file and rebuilds its positions on first access.
The reference uses append-mode streams so new records go to the end.

## Go design

`partitionLog` owns the open `os.File` and its recovered positions.
`openPartitionLog` creates the file when needed, scans it with `rebuildIndex`,
and opens it in append mode without truncating existing data. If recovery fails,
it closes the file and returns an error instead of exposing partial state.

## Tests

Filesystem tests use temporary directories to verify file creation, restart
index recovery, data preservation, append positioning, open errors, and cleanup
after a read failure.

## Deliberate differences

The reference caches each index in a process-wide map keyed by topic and
partition. This step returns one owned log value; a later broker function will
decide how partition logs are cached and selected.
