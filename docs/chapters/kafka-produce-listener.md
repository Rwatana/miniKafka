# Serving Produce through the Kafka listener

## Kafka concept

Kafka brokers accept TCP clients and process their request streams on persistent
connections. The listener must pass partition append behavior to each connection
so Produce requests can reach broker-owned storage.

## Go design

`ServeKafka` forwards an optional `KafkaBatchAppender` to the connection loop
for every accepted client. Listener shutdown and accept errors keep their
existing behavior.

## Tests

A TCP integration fixture sends Produce v3 over a real local listener and
checks the returned base offset.

## Deliberate limits

The application startup still needs to supply its broker method as the
appender. That final wiring is a separate step.
