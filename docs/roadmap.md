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
| 22 | `DashboardHandler.ServeHTTP` | Render the dashboard shell; its client reads topics and lag through the broker API. |
| 23 | `NewRouter` | Register the five broker endpoints and dashboard root path. |
| 24 | `PrepareDatabase` | Register PostgreSQL, verify the connection, and apply the offset schema at startup. |
| 25 | `Broker.Close` | Release the broker's cached partition log handles during shutdown. |
| 26 | `main` | Wire the broker, file logs, PostgreSQL, HTTP routes, and dashboard. |

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
milestones, each split into function-sized Issues and PRs. Apache Kafka protocol
compatibility is tracked separately below as a broader implementation goal.

The reference feature map is now implemented through the runnable Compose
stack. Further work extends the teaching project with Kafka-inspired storage
and throughput concepts; those extensions are identified as such instead of
being described as behavior from the Node reference.

| Order | Function or single change | Learning outcome |
| --- | --- | --- |
| 27 | `AppendRecords` | Encode multiple records, preserve contiguous offsets, and append one batch with one file write. |
| 28 | `Broker.ProduceBatch` | Pass a batch through broker validation and partition-log ownership while keeping offset assignment serialized. |
| 29 | `kafka.ReadRequestFrame` | Read one bounded, size-prefixed Kafka 4.4.0 TCP request before decoding its API header. |
| 30 | `kafka.DecodeRequestHeaderV1` | Decode the API key, schema version, correlation ID, and nullable client ID for non-flexible Kafka requests. |
| 31 | `kafka.decodeUnsignedVarint` | Decode bounded unsigned varints used by Kafka compact field lengths without confusing them with zig-zag VARINT. |
| 32 | `kafka.decodeCompactNullableString` | Decode Kafka's length-plus-one compact nullable UTF-8 strings and preserve following fields. |
| 33 | `kafka.skipTaggedFields` | Validate and skip Kafka flexible-protocol tag buffers while preserving following fields. |
| 34 | `kafka.DecodeRequestHeaderV2` | Decode flexible request headers using compact client IDs and tagged fields. |
| 35 | `kafka.encodeResponseHeaderV0` | Encode the four-byte correlation ID in a non-flexible Kafka response header. |
| 36 | `kafka.encodeResponseFrame` | Prefix a response body with its bounded four-byte Kafka message size. |
| 37 | `kafka.decodeCompactString` | Decode a required compact UTF-8 string while rejecting the null sentinel. |
| 38 | `kafka.decodeAPIVersionsRequestV3` | Parse ApiVersions v3 client identity fields and its flexible tagged-fields section. |
| 39 | `kafka.encodeAPIVersionsResponseV3` | Encode the flexible ApiVersions v3 response body and supported API version ranges. |
| 40 | `kafka.encodeAPIVersionsResponseFrameV3` | Assemble the v0 response header, ApiVersions v3 body, and bounded outer frame. |
| 41 | `kafka.HandleAPIVersionsRequestV3` | Validate one v3 request and return its correlation-matched response frame. |
| 42 | `kafka.ServeAPIVersionsV3Connection` | Process ordered, size-delimited ApiVersions requests until the connection reaches EOF. |
| 43 | `kafka.ServeAPIVersionsV3` | Accept TCP clients and run the ApiVersions v3 connection loop for each client. |
| 44 | `main` | Open the Kafka protocol listener alongside the existing HTTP application and expose port 9092 in Compose. |
| 45 | `kafka.decodeMetadataRequestV0` | Decode Metadata v0's legacy topic array and UTF-8 STRING fields. |
| 46 | `kafka.encodeMetadataResponseV0` | Encode broker, topic, and partition metadata using the legacy v0 schema. |
| 47 | `kafka.HandleMetadataRequestV0` | Match requested topics, return unknown-topic errors, and frame a correlation-matched Metadata v0 response. |
| 48 | `metadataTopicsFromPartitions` | Convert discovered single-broker log partitions into deterministically ordered Kafka topic metadata. |
