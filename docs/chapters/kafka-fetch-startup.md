# Wiring Fetch into broker startup

## Kafka concept

Clients discover a broker's supported API versions through ApiVersions before
they issue operations. A server must advertise only versions its listener can
actually handle, and the handler must reach the broker's partition storage.

## Go design

`main` advertises Fetch v4 and starts `ServeKafkaWithFetchReader`. It supplies
the broker's Kafka batch append and read methods, so Produce and Fetch operate
on the same durable partition logs.

## Tests

Startup tests verify the advertised Fetch range and invoke the read callback
for a missing partition to confirm that the live broker callback is present
and returns an empty result without creating a log.

## Deliberate limits

The listener advertises Fetch v4, but Kafka compatibility is still partial.
Transactions, compression, long polling, full byte budgeting, offsets and
metadata errors, groups, and replication remain unimplemented.
