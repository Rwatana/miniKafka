# Wiring Produce into broker startup

## Kafka concept

Clients discover API support through ApiVersions, then send Produce requests to
the broker. The advertised range must match APIs that the running listener can
actually handle.

## Go design

`main` advertises Produce v3 and passes the broker's Kafka batch append method
to the TCP listener. The existing Metadata and ApiVersions ranges remain
available.

## Tests

The startup test checks the advertised Produce version and confirms exactly one
broker-backed appender reaches the listener.

## Deliberate limits

This wires non-transactional, uncompressed Produce v3. Fetch and the remaining
Kafka APIs are not implemented yet.
