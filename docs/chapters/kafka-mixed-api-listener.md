# Accepting mixed Kafka API clients

## Kafka concept

A Kafka broker accepts TCP clients and handles each connection independently.
The connection carries multiple framed requests, while different clients can
exchange requests at the same time.

## Go design

`ServeKafka` accepts clients concurrently and runs the mixed-API connection
loop for each one. The listener owns connection closure, and a caller-provided
metadata source supplies a current topic snapshot when a client asks for
Metadata.

## Tests

Loopback TCP coverage sends ApiVersions v3 and Metadata v0 through one client,
checks the metadata-provider call boundary, and verifies normal listener
shutdown and accept errors.

## Deliberate limits

Application startup still uses the prior ApiVersions-only listener. A later
step will wire this listener to the file-backed broker metadata source.
