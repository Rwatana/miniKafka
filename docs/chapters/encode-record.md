# Encoding a MiniKafka record

## Kafka concept

A partition log is an ordered sequence of records. The reference MiniKafka
stores each record as a JSON object followed by a newline, so consecutive
records can be appended and read one line at a time. The record carries its
partition offset, optional key, value, and timestamp.

## Reference source

The reference implementation is in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js).
Its append path writes a JSON serialization and a newline to the partition's
`.log` file.

## Go design

`internal/storage.Record` uses a `*string` key so a missing key serializes as
JSON `null`. Its timestamp is an `int64` Unix millisecond value, matching the
reference's `Date.now()`. `encodeRecord` uses Go's JSON encoder for correct
escaping and appends exactly one newline. The function only prepares bytes;
assigning offsets and writing to a file are separate behaviors.

## Tests

The unit test decodes both keyed and null-key output, verifies every record
field, checks UTC timestamp normalization, and asserts exactly one terminating
newline.

## Deliberate differences

The record contract preserves the reference's numeric Unix millisecond
timestamp instead of substituting a Go `time.Time` string. This project does
not implement Kafka's binary wire protocol.
