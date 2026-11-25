# HTTP produce handler

## Kafka concept

The producer API accepts a topic, optional partition and key, and a value. It
returns the offset assigned by the partition log.

## Reference source

`POST /produce` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
defaults the partition to zero, requires topic and value, calls the log append,
and responds with topic, partition, and offset.

## Go design

`ProduceHandler` validates and decodes the JSON request, applies the same
partition default, calls `Broker.Produce`, and encodes the reference response.
It remains a transport adapter and does not import storage directly.

## Tests

`httptest` covers default and explicit partitions, keyed input, invalid JSON,
missing fields, negative partitions, and broker failures.

## Deliberate differences

The Go handler returns HTTP 400 for negative partitions, malformed JSON, and
missing fields. Broker/storage failures return HTTP 500 with a JSON error.
