# Exposing the partition log opener

## Go package concept

An unexported function can be used only inside its Go package. The broker needs
to open a storage log through the storage package's boundary instead of
reimplementing file creation and index recovery.

## Change

`PartitionLog` is now an exported type with private fields, and
`OpenPartitionLog` is the exported entry point. This PR changes visibility
without changing how files are opened, indexed, or recovered.

## Tests

The existing storage package tests are updated to use the exported names and
continue to exercise file creation, recovery, append mode, and errors. A later
broker package can now hold the returned `*storage.PartitionLog`.

## Deliberate differences

The file handle and index stay private so callers must use storage operations
instead of editing internal state directly.
