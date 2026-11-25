# MiniKafka Roadmap

The first end-to-end milestone is a single broker, one topic and one partition,
with persistent append and offset-based fetch. Each implementation Pull Request
contains one production function, its tests, and the matching chapter note.

| Order | Function | Learning outcome |
| --- | --- | --- |
| 1 | `encodeFrame` | Store opaque event bytes behind a bounded length prefix |
| 2 | `readFrame` | Read frames and report EOF or damaged input clearly |
| 3 | `Open` | Recover a partition log after restart |
| 4 | `Close` | Release the file resource owned by a log |
| 5 | `Append` | Persist an event and assign the next offset |
| 6 | `Fetch` | Read records in offset order from an inclusive start |
| 7 | producer handler | Accept an event over HTTP |
| 8 | consumer handler | Return events from an offset over HTTP |
| 9 | `main` | Compose a loopback single-broker process |

The later extension path is CRC validation, segment rotation, sparse indexes,
multiple topics and partitions, batching, consumer positions, and replication.
Each extension should be broken into one-function changes with explicit tests.
