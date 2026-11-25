# Loading a committed consumer offset

## Kafka concept

Consumer groups resume from the offset they last committed for each topic
partition. A group with no committed position starts from offset zero in this
MiniKafka implementation.

## Reference source

The reference `consumer_offsets` table uses
`(consumer_group, topic, partition)` as its key. The `/consume` route consults
that row when the request does not include an explicit offset.

## Go design

`GetCommittedOffset` selects one row with all three key fields. It treats
`sql.ErrNoRows` as offset zero and wraps other database errors with the
partition identity.

## Tests

Tests use a small `database/sql/driver` fixture to check the returned stored
offset, the full lookup key, the zero default, validation, and query errors.

## Deliberate differences

Offset selection is exposed here as a storage function. The HTTP consume
handler will connect explicit offsets and group lookups in a later Issue.
