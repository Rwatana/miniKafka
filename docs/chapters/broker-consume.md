# Consuming through the broker

## Kafka concept

Consumers read records from an inclusive partition offset. The broker uses the
partition's byte index so storage can seek directly and read only a bounded
number of lines.

## Reference source

`readFrom` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
selects the cached topic partition and reads from the requested offset. The
`GET /consume` route in `broker/server.js` supplies the offset and maximum.

## Go design

`Broker.Consume` validates the selection, reuses an open log or opens and
indexes an existing one, then delegates to `storage.ReadFromOffset`. A
partition that has never been written returns an empty result without creating
a log file.

## Tests

Tests cover inclusive bounded reads, restart recovery, missing partitions,
invalid arguments, and filesystem failures.

## Deliberate differences

This first broker function accepts an explicit starting offset. A later
consumer-group Issue will use PostgreSQL when the HTTP request omits an offset.
