# Exposing partition offset reads

## Go package concept

The transport layer should delegate reads to storage instead of seeking files
or decoding persisted records itself.

## Change

`ReadFromOffset` exposes the existing bounded read operation for
`*storage.PartitionLog`. It returns decoded records while keeping the file and
index private.

## Tests

The storage tests continue to cover inclusive offsets, bounds, end-of-log,
blank lines, malformed data, truncation, and I/O failures.

## Deliberate differences

The reference accepts a topic and partition and resolves its cache internally.
This function accepts an already-open partition so a later broker function can
choose the right log before delegating the read.
