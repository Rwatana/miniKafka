# Producing a batch through the broker

## Kafka concept

A broker mediates writes to a partition: it owns the partition log, preserves
offset assignment order, and returns offsets to the producer. This step carries
the batched append primitive through that ownership boundary.

## Reference source

The reference project has one-record `produce` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js),
which calls the one-record append in `broker/log.js`. `ProduceBatch` is a Go
extension; the reference endpoint remains unchanged.

## Go design

`(*Broker).ProduceBatch` validates the same topic and partition inputs as
`Produce`, takes the broker mutex, opens or reuses one partition log, then
delegates the entire slice to `storage.AppendRecords`. Empty batches return
without creating a directory or log file.

## Tests

Broker tests cover an ordered round trip, empty input, invalid configuration,
directory and open failures, append errors, and concurrent batches receiving
unique contiguous offsets.

## Deliberate differences

The Node MiniKafka endpoint still accepts one record per request. This method
does not add a public HTTP batch API or Kafka wire protocol support.
