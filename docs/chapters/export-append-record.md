# Exposing partition record appends

## Go package concept

Sibling packages communicate through exported identifiers. The broker needs a
single storage operation that owns offset assignment, record serialization,
file append, and index advancement.

## Change

`AppendRecord` exposes the already-tested append operation for
`*storage.PartitionLog`. It returns the assigned offset and keeps the log's
file handle and position slice private.

## Tests

The existing storage tests still verify sequential offsets, numeric millisecond
timestamps, append-only persistence, recovery after reopening, and failure
paths.

## Deliberate differences

The reference accepts topic and partition names and resolves its process-wide
log cache internally. The broker layer will select a `PartitionLog` and call
this storage operation.
