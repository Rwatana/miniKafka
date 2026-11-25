# Exposing the partition log close operation

## Go package concept

The package that creates a resource should provide its public release operation
so callers can follow the ownership contract without reaching into private
fields.

## Change

`ClosePartitionLog` exposes the existing close behavior for
`*storage.PartitionLog`. It keeps nil-safe handling and contextual close errors
while leaving the file descriptor itself private.

## Tests

The storage tests continue to cover nil values, a successful close, and an
operating-system error when the descriptor is closed twice.

## Deliberate differences

The reference keeps its write streams in module-level maps. The Go API makes
each log's owner responsible for closing it, which the broker can use during
shutdown.
