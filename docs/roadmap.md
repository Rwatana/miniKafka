# MiniKafka Roadmap

This roadmap follows the feature set of
[ayuxh16/Minikafka](https://github.com/ayuxh16/Minikafka/tree/main/minikafka)
and implements it in Go. It starts with durable partition records and adds
broker APIs, group offsets, lag, and the small dashboard only after the
underlying behavior is available.

Every numbered implementation step is tracked by its own GitHub Issue and PR.
Each implementation PR contains one production function, its tests, and the
matching learning note in one commit. The bootstrap and roadmap Issues are
documentation/process changes and do not count as broker behavior.

## Reference feature map

| Reference behavior | Go implementation boundary |
| --- | --- |
| Append-only `.log` file per topic partition | Partition log in `internal/storage`, backed by `internal/infrastructure/file` |
| JSON record with offset, optional key, value, and timestamp | Record encoding and decoding in `internal/storage` |
| In-memory offset-to-byte-position index rebuilt on startup | Partition log recovery/index functions in `internal/storage` |
| Produce, consume, commit, topics, and lag endpoints | HTTP adapters calling `internal/broker` |
| PostgreSQL committed offset keyed by group, topic, and partition | Offset schema and persistence in `internal/infrastructure/postgres` |
| Topic/partition/lag dashboard | Go template and static assets using the HTTP API |

## Ordered implementation steps

Each row is a separate Issue/PR. A step may add a type or test fixture needed
by its function, but must not add a second production function.

| Order | Function or single change | Learning outcome |
| --- | --- | --- |
| 1 | `encodeRecord` | Serialize the reference record fields as one newline-delimited JSON record. |
| 2 | `decodeRecord` | Parse one record and reject malformed persisted data. |
| 3 | `rebuildIndex` | Scan a partition log and map offsets to byte positions. |
| 4 | `OpenPartitionLog` | Open or create a partition log and recover its next offset. |
| 5 | `closePartitionLog` | Release the file resource owned by one partition log. |
| 6 | `appendRecord` | Append one record and assign the next sequential offset. |
| 7 | `latestOffset` | Return the next offset to write from the number of indexed records. |
| 8 | `readFromOffset` | Seek via the index and return ordered records from an inclusive offset. |
| 9 | `Broker.Produce` | Select/cache a topic partition and delegate an append to storage. |
| 10 | `Broker.Consume` | Select a partition and delegate a bounded read to storage. |
| 11 | `produceHandler` | Implement `POST /produce` for a topic and partition. |
| 12 | `ConsumeHandler.ServeHTTP` | Implement `GET /consume` with an explicit or committed starting offset. |
| 13 | `CommitOffset` | In `internal/infrastructure/postgres`, add the reference composite-key table and atomically upsert one group's committed partition position. |
| 14 | `GetCommittedOffset` | Load a group's position for a topic partition, defaulting to zero before the first commit. |
| 15 | `CommitHandler.ServeHTTP` | Implement `POST /commit` and delegate durable offset writes to PostgreSQL. |
| 16 | `Broker.ListPartitions` | Discover existing topic/partition names by scanning `.log` files in the broker data directory. |
| 17 | `Broker.LatestOffset` | Return the next offset for one partition, recovering its log when needed. |
| 18 | `TopicsHandler.ServeHTTP` | Implement `GET /topics` with each partition's next offset. |
| 19 | `ListOffsetsForGroup` | Load all committed partition positions for one consumer group. |
| 20 | `CalculateLag` | Calculate latest offset minus committed position per partition. |
| 21 | `LagHandler.ServeHTTP` | Implement `GET /lag` using committed offsets and partition log state. |
| 22 | `dashboardHandler` | Render the dashboard shell; its static client reads topics and lag through the broker API. |
| 23 | `main` | Wire the broker, file logs, PostgreSQL, HTTP routes, and dashboard. |

## First end-to-end milestone

The broker milestone is complete when an automated integration test can:

1. Produce multiple records and observe sequential offsets.
2. Consume from an explicitly selected offset and verify order.
3. Restart the broker, rebuild the in-memory index, and continue offsets.
4. Commit a consumer-group offset in PostgreSQL and consume from it.
5. Query lag and verify the latest, committed, and lag values.

## Deliberate limits and later work

The first complete version remains a single broker with one file per partition.
It has no replication, consumer-group rebalancing, retention, or compaction,
matching the reference project's intentional scope. These are separate future
milestones, each split into function-sized Issues and PRs. Kafka wire protocol
compatibility is also outside this teaching project's scope.
