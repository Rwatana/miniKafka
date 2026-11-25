# Calculating consumer lag

## Kafka concept

Consumer lag counts how many records remain between a consumer group's
committed position and the end of a partition log.

## Reference source

`GET /lag` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
calculates lag as `latestOffset - committedOffset` for each committed
partition.

## Go design

`broker.CalculateLag` is a pure arithmetic function. Keeping the calculation
separate from file and database I/O makes the meaning of the two offsets easy
to test.

## Tests

Tests cover a consumer behind, a caught-up consumer, an empty partition, and a
committed position ahead of the current log end.

## Deliberate differences

The function preserves a negative result if a committed position is ahead of
the log end, matching the reference formula instead of silently clamping it.
