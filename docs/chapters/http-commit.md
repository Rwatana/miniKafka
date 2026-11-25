# Committing offsets over HTTP

## Kafka concept

Committing records the next position a consumer group should resume from for a
topic partition. The commit operation is scoped by group, topic, and partition.

## Reference source

`POST /commit` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
accepts `{group, topic, partition=0, offset}` and returns the committed key and
offset.

## Go design

`CommitHandler.ServeHTTP` validates the JSON fields and calls an injected
committer. The PostgreSQL adapter can be wired without adding a database
dependency to the HTTP layer.

## Tests

Tests cover default partition zero, explicit zero offsets, required and invalid
fields, method validation, missing wiring, and persistence failures.

## Deliberate differences

Malformed JSON and negative numeric values return HTTP 400 before reaching the
database. The reference route delegates many such errors to generic error
handling.
