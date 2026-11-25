# Architecture and Development Rules

## Purpose

MiniKafka is a teaching implementation of the smallest useful Kafka-like path:
one producer appends an event to one broker's partition log, and a consumer reads
it again by offset. The code grows only when a tested behavior needs another
boundary.

## Dependency direction

```text
cmd / adapter / infrastructure
               ↓
          broker/usecase
               ↓
          storage/domain
```

- `internal/storage` owns record framing and offset-independent storage rules.
- `internal/infrastructure/file` owns operating-system file access.
- `internal/broker` composes storage operations when a broker behavior exists.
- `internal/adapter` translates transport requests to broker calls.
- `cmd/minikafka` is the composition root and owns process startup.

Do not create these packages empty or add interfaces before a caller needs them.
The storage core must not import HTTP or filesystem adapters.

## Learning path

1. Encode one bounded length-prefixed record frame.
2. Read one complete frame and distinguish clean EOF from corruption.
3. Open one partition log and recover its next offset.
4. Close the owned log file.
5. Append an event and return its monotonically increasing offset.
6. Fetch a bounded ordered range from an inclusive offset.
7. Add producer and consumer HTTP handlers, then a loopback server.
8. Verify append, fetch, restart, and continued offsets end to end.

After the single-broker path works, future small steps can add checksums, segment
rotation, sparse indexes, topic partitioning, batches, consumer positions, and
replication. These are not part of the first implementation unit.

## Test and documentation rules

Use Red-Green-Refactor: name a user-visible behavior in a failing test, implement
only enough to pass, and then improve the design without changing behavior. Use
unit tests for pure rules and temporary directories for filesystem integration.
Each function-sized change documents the relevant Kafka concept, source mapping,
design decisions, tests, and deliberate omissions in `docs/chapters/`.
