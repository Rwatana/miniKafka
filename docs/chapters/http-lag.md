# Reporting consumer lag over HTTP

## Kafka concept

Consumer lag compares a group's committed position with the current end of each
partition. Reporting both offsets makes the lag value explainable to a
dashboard or operator.

## Reference source

`GET /lag` in
[`broker/server.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/server.js)
requires a group, lists committed positions, and calculates each partition's
latest offset and lag.

## Go design

`LagHandler.ServeHTTP` injects two boundaries: a committed-offset list lookup
and a broker latest-offset lookup. It combines those with
`broker.CalculateLag` and returns the reference response fields.

## Tests

Tests cover valid multi-partition results, negative lag, empty groups, method
and query validation, missing wiring, and both dependency failures.

## Deliberate differences

The handler calculates lag with the same raw subtraction as the reference and
does not clamp a negative value.
