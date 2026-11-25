# Closing a partition log

## Kafka concept

A broker keeps partition log files open while they are active to support
sequential writes. When a log is no longer in use or the broker shuts down, its
owned file descriptor must be released.

## Reference source

The reference keeps write streams in `log.js` for reuse. This Go function
defines the matching lifecycle boundary for the file owned by `partitionLog`.

## Go design

`closePartitionLog` closes the file stored on the log value and returns a
contextual error from the operating system. A nil log or nil file is already
closed. It does not remove the log from a broker cache; cache lifecycle is a
separate behavior.

## Tests

Tests cover nil state, successful descriptor release, and an operating-system
error from closing an already closed file.

## Deliberate differences

The reference retains process-wide write streams and does not expose a close
operation in its small broker API. The Go project makes descriptor ownership
explicit so tests and a later graceful shutdown can release resources.
