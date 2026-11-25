# Producing through the broker

## Kafka concept

A producer writes to a topic partition. The broker selects the partition log,
keeps it available for reuse, and returns the offset assigned by storage.

## Reference source

The `/produce` route in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
defaults the partition to zero and calls `log.append`. The storage path lazily
opens and caches one file per topic and partition.

## Go design

`Broker.Produce` validates the topic and partition, lazily opens the matching
`<topic>__<partition>.log`, and delegates the append to
`storage.AppendRecord`. A mutex serializes concurrent calls on one broker so
they cannot assign the same offset.

## Tests

Tests verify offsets across repeated produce calls, independent partitions,
recovery after a new broker uses the same data directory, concurrent offset
uniqueness, invalid inputs, filesystem failures, and append errors.

## Deliberate differences

The reference exposes the HTTP route directly over its log module. This Go
function lives in a broker package so a later HTTP adapter can remain separate
from file storage.
