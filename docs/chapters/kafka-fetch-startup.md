# Wiring Fetch into broker startup

## Kafka concept

Clients discover a broker's supported API versions through ApiVersions before
they issue operations. A server must advertise only versions its listener can
actually handle, and the handler must reach the broker's partition storage.

## Go design

`main` advertises Fetch v4 and ListOffsets v1 and starts
`ServeKafkaWithFetchReader`. It supplies the broker's Kafka batch append, read,
and timestamp lookup methods, so Produce, Fetch, and ListOffsets operate on the
same durable partition logs. When the HTTP server exits, startup closes the
Kafka listener and waits for its accept loop to stop before returning.

## Tests

Startup tests verify the advertised Fetch and ListOffsets ranges and invoke
both callbacks for a missing partition to confirm that the live broker methods
are connected and return a typed missing-partition error.

## Deliberate limits

The listener advertises a limited Kafka API subset. Transactions, compression,
several consumer APIs, groups, and replication remain unimplemented.
