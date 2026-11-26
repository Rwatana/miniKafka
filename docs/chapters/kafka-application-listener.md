# Starting the Kafka listener from the application

## Kafka concept

A broker process exposes a Kafka protocol listener on its configured address.
The listener accepts client connections independently of other application
interfaces.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Kafka as a binary protocol over TCP. The ApiVersions request is the
initial capability negotiation used by clients to select supported schemas.

## Go design

The executable reads `KAFKA_LISTEN_ADDR`, defaulting to `:9092`, opens a TCP
listener, and serves ApiVersions versions 0 through 3 alongside the existing
HTTP routes. Compose publishes port 9092 for local clients.

## Tests

Application tests verify the default/custom address, the advertised ApiVersions
range, listener cleanup, and startup failure handling.

## Current limit

The application now opens a Kafka protocol port, but it only handles
ApiVersions v3 requests. Metadata, Produce, Fetch, KRaft, replication, and the
other Kafka broker features remain unimplemented.
