# Serving consumer reads over HTTP

## Kafka concept

Each consumer group resumes from its committed position unless the client
chooses an explicit offset for a read. The broker then returns a bounded batch
from that inclusive position.

## Reference source

`GET /consume` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
requires `topic` and `group`, defaults the partition to zero and maximum batch
to 50, and returns `topic`, `partition`, `fromOffset`, and `messages`.

## Go design

`ConsumeHandler.ServeHTTP` validates the query, uses a requested offset when
present, otherwise calls the injected committed-offset lookup, then delegates
to `Broker.Consume`. Keeping the lookup injectable lets the application wire
the PostgreSQL adapter without coupling the HTTP package to a database
connection.

## Tests

Handler tests cover defaults, committed and explicit offsets, required fields,
invalid numbers, lookup errors, broker errors, and empty batches.

## Deliberate differences

Malformed and negative numeric query values return HTTP 400. The JavaScript
route relies on `parseInt` and lets downstream failures reach its generic
HTTP 500 handler.
