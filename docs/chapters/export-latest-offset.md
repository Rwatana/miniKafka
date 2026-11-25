# Exposing the partition next-offset query

## Go package concept

Broker code should use storage queries rather than reading the storage index's
private fields.

## Change

`LatestOffset` exposes the existing query for `*storage.PartitionLog`. It
returns the number of indexed records, which is the next offset to assign.

## Tests

The storage tests continue to verify zero for an empty log, the next offset
after appends, and errors for invalid state.

## Deliberate differences

The reference takes topic and partition names and resolves a cached log. The
broker layer will choose the `PartitionLog` before asking storage for its next
offset.
