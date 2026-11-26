# Serving Fetch on a Kafka connection

## Kafka concept

Kafka clients keep TCP connections open and send multiple request frames on the
same socket. The broker reads each size-prefixed frame and responds with the
matching correlation ID before processing the next request.

## Go design

`ServeKafkaConnectionWithFetchReader` is the reader-aware connection loop. It
processes requests in order, refreshes metadata for Metadata and FindCoordinator
requests, dispatches Fetch through the batch reader and ListOffsets through the
optional offset lookup callback, and writes complete response frames even when
a socket performs partial writes. FindCoordinator needs the broker list to
return a coordinator endpoint to a real client connection.

## Tests

Connection tests send Fetch and ApiVersions frames over one `net.Pipe`, verify
Fetch and ListOffsets callback arguments and correlation IDs, check that
FindCoordinator loads and returns the broker endpoint, and cover protocol,
metadata, missing callback, and socket write failures.

## Deliberate limits

The TCP accept loop does not call this new entry point yet. Listener wiring and
startup advertisement remain separate steps.
