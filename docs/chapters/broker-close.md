# Closing broker partition logs

## Kafka concept

The broker keeps partition files open so appends can proceed sequentially.
When the broker shuts down, it must release the file handles it owns.

## Reference source

The reference `broker/log.js` caches one write stream per topic partition. Its
process lifecycle eventually releases those streams; Go makes the cleanup
explicit through the broker owner.

## Go design

`Broker.Close` serializes shutdown with produce and consume operations, closes
all cached partition logs, removes every cache entry, and joins contextual
errors after attempting all handles.

## Tests

Tests cover multiple healthy logs, repeated close, nil and empty brokers, and
one failing close alongside a healthy handle.

## Deliberate differences

The Go broker exposes an explicit close method because file handles are owned
by Go objects rather than process-managed write streams.
