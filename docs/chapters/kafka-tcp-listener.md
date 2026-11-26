# Accepting Kafka TCP connections

## Kafka concept

A Kafka broker listens for TCP clients and creates a per-connection request
loop. Connections can be closed independently while the listener continues to
accept new clients.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines the broker protocol as binary TCP with size-delimited requests and
ordered responses on each connection.

## Go design

`ServeAPIVersionsV3` accepts from a supplied `net.Listener`, starts one
ApiVersions v3 connection loop per client, and closes each connection after
the loop ends. Closing the listener is a clean shutdown; other accept errors
are returned to the caller.

## Tests

A localhost TCP test sends an ApiVersions request and verifies the framed
response. Listener tests cover clean close and a non-close accept failure.

## Deliberate differences

This entry point currently implements only the ApiVersions v3 negotiation
path. It does not yet register Metadata, Produce, Fetch, or other Kafka APIs.
