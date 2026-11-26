# Architecture and Development Rules

## Purpose

MiniKafka is a teaching implementation in Go of the core path in the
[reference MiniKafka project](https://github.com/ayuxh16/Minikafka/tree/main/minikafka):
a producer appends records to topic partitions, consumers read them by offset,
and consumer groups persist their committed positions. The implementation grows
in small, reviewed steps so each Kafka concept can be understood on its own.

This is a single-broker learning project. It does not claim wire compatibility
with Apache Kafka or implement a Kafka cluster.

## Reference behavior

The reference project provides these core pieces:

- Each topic partition has an append-only `.log` file. A record stores its
  offset, optional key, value, and timestamp.
- An in-memory index maps record offsets to byte positions in the partition
  file. Startup rebuilds this index by scanning the log.
- The broker exposes `POST /produce`, `GET /consume`, `POST /commit`,
  `GET /topics`, and `GET /lag` over HTTP.
- PostgreSQL stores one committed offset for each
  `(consumer_group, topic, partition)` tuple. Commit uses an atomic upsert so
  the tuple is inserted once and updated on subsequent commits.
- A small dashboard reads the broker API to display topics, partitions, and
  consumer lag.

The initial Go implementation keeps the reference's deliberate limits: one
broker, one log file per partition, no replication, no consumer group
rebalancing, and no retention or compaction. These limits keep the storage and
consumer position rules visible while the end-to-end path is built.

## Package boundaries

```text
cmd/minikafka
   ├──→ internal/adapter/http ──→ internal/broker ──→ internal/storage
   └──→ internal/infrastructure/postgres
```

- `internal/storage` defines record encoding, the offset index, and partition
  log behavior without importing HTTP or PostgreSQL.
- `internal/storage` owns filesystem operations for durable partition logs and
  keeps the byte index beside the log behavior.
- `internal/infrastructure/postgres` owns the SQL schema and committed-offset
  persistence. The application passes its functions into HTTP handlers.
- `internal/broker` coordinates partition operations and offset calculations.
- `internal/adapter/http` translates HTTP requests and responses to broker
  calls.
- `internal/protocol/kafka` implements versioned Kafka TCP wire formats. The
  existing HTTP adapter remains the educational MiniKafka API while Kafka
  protocol compatibility is added in separate vertical slices.
- `cmd/minikafka` wires dependencies and starts the process.
- The dashboard is served by Go's `html/template` and static files. It calls
  the same HTTP API as external clients; it does not introduce another broker
  path.

Create a package only when an Issue introduces a caller for it. Avoid unused
interfaces and empty scaffolding.

## End-to-end acceptance path

The first complete milestone must demonstrate all of the following:

1. Produce records to a topic partition and receive sequential offsets.
2. Consume from an explicitly requested offset and receive ordered records.
3. Restart the broker and rebuild the offset-to-byte-position index from the
   append-only log, preserving the next offset.
4. Commit a consumer group's offset and read that committed offset on a later
   consume request.
5. Report per-partition committed offset, latest offset, and lag from
   `GET /lag`.

The topic list endpoint must report the topics and partitions present on disk.
`GET /consume` uses an explicit offset when supplied; otherwise, when a group is
specified, it starts from that group's committed offset. `POST /commit` stores
the caller's next position for the selected group, topic, and partition.

## Function-sized workflow

- Start each behavior from a GitHub Issue with explicit acceptance criteria and
  tests.
- Keep each implementation PR to one production function, its tests, and the
  learning note for that function. Supporting declarations do not justify
  bundling another behavior.
- Keep exactly one commit in each PR. Amend it when review requests changes.
- Use `feature/#<issue-number>_<snake_case_description>` branches and include
  the matching Issue number and `Closes #<issue-number>` in the PR body.
- Use commit subjects in the form `[Scope] Summary.`.
- Review every PR against its Issue, function boundary, tests, explanation, and
  CI before it is considered complete. Do not skip review because the change is
  small.

## Tests and learning notes

Use Red-Green-Refactor: first write a failing test for the behavior, then make
the smallest implementation that passes, then refactor without changing the
behavior. Use temporary directories for filesystem tests and a disposable
PostgreSQL service for persistence integration tests. Keep statement coverage at
100 percent as required by the repository checks.

Each function-sized change adds or updates a note under `docs/chapters/` that
explains the Kafka concept, maps it to the reference source, describes the Go
design, records the tests, and names intentional differences. Explain editable
Draw.io diagrams in Markdown and commit their source files.
