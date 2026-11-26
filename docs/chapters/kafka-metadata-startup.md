# Wiring Kafka metadata into application startup

## Kafka concept

After a client negotiates the supported APIs, it requests Metadata and uses the
advertised broker endpoint plus partition placement to route later requests.
The advertised hostname must be reachable from the client network.

## Go design

`main` creates the file-backed broker before starting the Kafka listener. It
advertises Metadata v0 alongside ApiVersions v0-v3 and supplies a callback that
discovers partition log files for every Metadata request. The callback maps
the single broker's partitions to leader, replica, and ISR fields. Set
`KAFKA_ADVERTISED_HOST` when clients reach the broker through a hostname other
than `localhost`; the bound TCP port is advertised automatically.

## Tests

Startup tests verify both advertised API ranges, a non-nil live metadata
provider, the configurable advertised host, and the actual listener port.

## Deliberate limits

The running Kafka listener now serves ApiVersions and Metadata. Produce,
Fetch, record batches, replication, and consumer-group APIs remain separate
implementation work.
