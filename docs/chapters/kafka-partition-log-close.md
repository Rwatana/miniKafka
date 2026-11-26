# Closing Kafka partition logs

## Kafka concept

Partition log segments are owned by the broker and must be closed during
shutdown so the operating system releases file descriptors and the broker can
reopen them cleanly.

## Go design

`CloseKafkaPartitionLog` closes the file owned by the Kafka log handle and
returns a contextual error if the handle is nil or the operating system rejects
the close.

## Tests

Fixtures cover a successful close, a second close, and a nil or incomplete
handle.

## Deliberate limits

This function closes one handle. Broker shutdown iteration and segment rotation
are separate features.
