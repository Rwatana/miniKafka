# Committing consumer offsets

## Kafka concept

A committed offset is the consumer group's durable position in one topic
partition. The group, topic, and partition together identify one position.

## Reference source

The reference schema in
[`db/schema.sql`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/db/schema.sql)
uses a composite primary key over `consumer_group`, `topic`, and `partition`.
The `/commit` route writes the committed offset for that key.

## Go design

`CommitOffset` uses `database/sql` and PostgreSQL's `ON CONFLICT` clause. One
statement inserts a new position or atomically updates the existing position;
the table definition is kept beside the adapter for later startup wiring.

## Tests

Tests verify the exact group/topic/partition/offset arguments, the conflict
update contract, input validation, and wrapped database errors. The executor
interface keeps this function testable without requiring a running database.

## Deliberate differences

The database driver and connection lifecycle are added in a later wiring step.
This function expects the schema to have been applied before it is called.
