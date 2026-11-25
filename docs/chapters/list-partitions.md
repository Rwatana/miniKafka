# Discovering partitions from log files

## Kafka concept

Topics contain partitions, and this implementation stores each topic partition
in a separate append-only `.log` file. Listing the files lets the dashboard
show partitions after a broker restart.

## Reference source

`listPartitions` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
scans the data directory and derives the topic and partition from each `.log`
filename.

## Go design

`Broker.ListPartitions` creates the data directory when needed, ignores
directories and filenames without a valid non-negative partition suffix, and
returns deterministic results from `os.ReadDir`.

## Tests

Tests cover multiple partitions, malformed and unrelated entries, missing
directories, nil or invalid broker setup, and directory read failures.

## Deliberate differences

The Go parser uses the final `__` separator and ignores malformed filenames.
This keeps topics containing `__` discoverable and avoids reporting a
non-numeric partition as a valid topic partition.
