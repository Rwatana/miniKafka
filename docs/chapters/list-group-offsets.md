# Listing committed positions for a consumer group

## Kafka concept

Consumer lag is measured per topic partition, so the monitoring path needs all
committed positions owned by a consumer group.

## Reference source

`listOffsetsForGroup` in
[`broker/offsets.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/offsets.js)
selects topic, partition, and committed offset rows filtered by group.

## Go design

`ListOffsetsForGroup` uses `database/sql` rows, scans one `GroupOffset` per
database row, and orders results by topic and partition for stable responses.
Query, scan, and iteration errors include the requested group.

## Tests

The test SQL driver covers ordered rows, empty groups, validation, query errors,
scan errors, and row iteration errors without requiring a PostgreSQL service.

## Deliberate differences

The query explicitly orders rows. The reference relies on database row order,
which PostgreSQL does not guarantee without `ORDER BY`.
