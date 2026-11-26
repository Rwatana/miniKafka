# Serving Produce requests on Kafka connections

## Kafka concept

Kafka clients reuse TCP connections for ordered requests across multiple API
keys. A Produce request must receive its response on that same connection, while
`acks=0` must not insert a response that would desynchronize later requests.

## Go design

`ServeKafkaConnection` accepts an optional batch appender and passes it into the
mixed-API dispatcher for each frame. It continues its request loop when the
Produce handler returns an empty response for `acks=0`.

## Tests

`net.Pipe` tests exercise a Produce request end to end through framing,
dispatch, append callback, and response writing, and verify that a following
ApiVersions response remains aligned after an `acks=0` Produce request.

## Deliberate limits

The TCP listener does not yet provide the broker callback; listener and startup
wiring remain separate steps.
