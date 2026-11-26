# Wiring Fetch into broker startup

## Kafka concept

Clients discover a broker's supported API versions through ApiVersions before
they issue operations. A server must advertise only versions its listener can
actually handle, and the handler must reach the broker's partition storage.

## Go design

`main` advertises Fetch v2 through v4, ListOffsets v1, and FindCoordinator v1 and starts
`ServeKafkaWithFetchReader`. It supplies the broker's Kafka batch append, read,
and timestamp lookup methods, so Produce, Fetch, and ListOffsets operate on the
same durable partition logs. The metadata provider supplies the broker endpoint
that FindCoordinator v1 returns for group coordinator discovery. When the HTTP
server exits, startup closes the Kafka listener and waits for its accept loop
to stop before returning.

## Tests

Startup tests verify the advertised Fetch, ListOffsets, and FindCoordinator
ranges and invoke the storage callbacks for a missing partition to confirm
that the live broker methods are connected and return a typed
missing-partition error.

## Deliberate limits

The listener advertises a limited Kafka API subset. Transactions, compression,
several consumer APIs, groups, and replication remain unimplemented.
