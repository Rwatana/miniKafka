# Reading a partition's latest offset through the broker

## Kafka concept

The latest offset is the next position a producer will receive. It is also the
record count in an append-only partition log and the upper bound used to
calculate consumer lag.

## Reference source

`latestOffset` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
loads the partition index and returns its length.

## Go design

`Broker.LatestOffset` serializes access to the broker's log cache, opens and
indexes an existing file on demand, and delegates the count to
`storage.LatestOffset`. A missing partition reports zero without creating a
file.

## Tests

Tests cover cached and recovered logs, missing files, invalid arguments,
filesystem failures, and storage open/read failures.

## Deliberate differences

The Go broker validates topic paths and wraps storage errors with the topic and
partition identity.
