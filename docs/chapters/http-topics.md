# Listing topic partitions over HTTP

## Kafka concept

Each partition tracks its own sequence of records and next offset. Broker
metadata should expose the partition and its current end position to monitoring
clients.

## Reference source

`GET /topics` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
lists the partitions discovered from log files and adds each partition's
`latestOffset`.

## Go design

`TopicsHandler.ServeHTTP` asks the broker for the file-backed partition list,
then reads each next offset and returns a JSON array in that deterministic
order.

## Tests

Tests cover multiple partitions, empty results, non-GET requests, listing
failures, and offset failures.

## Deliberate differences

The Go endpoint returns an empty JSON array when there are no partition files.
The underlying broker creates the data directory as the reference does.
