# Reading a partition's next offset

## Kafka concept

The next offset is the current number of records in a partition. Producers use
it for the next append, and topic summaries report it as the partition's latest
offset.

## Reference source

`latestOffset` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
loads the partition index and returns its length.

## Go design

`latestOffset` returns the length of the in-memory position index. This value is
the next offset to assign, one greater than the last existing record's offset.
The function reads no file bytes and does not confuse the next offset with the
last assigned offset.

## Tests

Tests cover an empty partition, three successful appends, and invalid log state.

## Deliberate differences

The reference accepts a topic and partition and loads the cache internally.
This storage function reads an already-open `partitionLog`; topic and partition
selection will be provided by the broker layer.
