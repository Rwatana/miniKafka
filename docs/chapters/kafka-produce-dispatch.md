# Dispatching Produce v3

## Kafka concept

Kafka API key 0 identifies Produce requests. The broker selects a schema by
the request's API version and routes each frame to the matching handler while
keeping mixed APIs on one TCP connection.

## Go design

`HandleKafkaRequest` dispatches API key 0 frames to `HandleProduceRequestV3`
and passes the optional partition batch appender through. A request without an
appender fails explicitly until the running listener is wired in a later step.

## Tests

Fixtures verify Produce v3 routing and appender arguments, plus explicit
failure when a caller does not provide the broker callback. Existing Metadata
and ApiVersions routing remains covered.

## Deliberate limits

The listener does not yet supply the appender, so this dispatcher support is
not yet reachable through the running TCP server.
