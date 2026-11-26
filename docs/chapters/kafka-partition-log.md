# Opening a Kafka partition log

## Kafka concept

Kafka stores each partition as an append-only binary log of record batches.
This storage path is separate from MiniKafka's JSON line log so the existing
HTTP reference behavior remains available while Kafka protocol storage is
introduced incrementally.

## Go design

`OpenKafkaPartitionLog` creates parent directories and opens the file with
append semantics and owner-only permissions. The handle reserves its next
offset state for subsequent recovery and append steps.

## Tests

Fixtures cover creating nested directories, opening existing files without
truncating them, appending to an existing file, file permissions, and invalid
paths.

## Deliberate limits

This step only opens the binary file. It does not parse existing record batches
to recover offsets or append protocol records; those are subsequent steps.
