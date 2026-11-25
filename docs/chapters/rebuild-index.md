# Rebuilding the partition byte index

## Kafka concept

Offsets identify records within a partition. A consumer needs the matching
byte position to seek directly into the append-only file without scanning from
its beginning on every read.

## Reference source

`loadIndex` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
scans a partition file once, records each line's starting byte position, and
uses the position array index as the record offset.

## Go design

`rebuildIndex` reads an `io.Reader` line by line and returns the starting
position for each line. It counts the bytes read, which matters because UTF-8
characters may occupy several bytes. It does not parse JSON or open a file;
both concerns stay in their own function-sized steps.

## Tests

Tests verify empty input, one line, multiple lines, a final line without a
newline, a terminated log without a phantom empty record, multibyte UTF-8 byte
positions, and a contextual error without a partial index.

## Deliberate differences

The reference scans a file stream asynchronously. This Go step accepts an
`io.Reader` so it can process large logs incrementally without holding the
whole file in memory. A later open/recovery function will open the file and
pass it to this index function. The persisted format uses LF line endings as
produced by `encodeRecord`.
