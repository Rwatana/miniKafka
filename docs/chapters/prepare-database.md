# Preparing PostgreSQL at startup

## Kafka concept

Consumer group positions must survive broker restarts. PostgreSQL stores one
committed offset for each group, topic, and partition tuple.

## Reference source

The reference broker creates a PostgreSQL pool from `DATABASE_URL`, and
[`db/schema.sql`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/db/schema.sql)
defines the `consumer_offsets` table.

## Go design

`PrepareDatabase` opens the pgx `database/sql` driver, verifies connectivity,
and applies the embedded schema. It closes the connection if startup checks
fail so callers receive only a ready database handle.

## Tests

Tests use a test SQL driver to exercise successful ping/schema setup, invalid
inputs, driver open failures, ping failures, and schema execution failures.

## Deliberate differences

The Go binary embeds and applies the schema during startup. The reference keeps
its schema in a SQL file for the database setup path.
