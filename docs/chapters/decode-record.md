# Decoding a MiniKafka record

## Kafka concept

Consumers and log recovery must turn persisted record bytes back into the
offset, optional key, value, and numeric Unix millisecond timestamp that a producer wrote. The
newline-delimited format lets a caller provide one log line at a time.

## Reference source

The reference reads each partition's `.log` file and parses JSON records in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js).

## Go design

`decodeRecord` removes one optional trailing newline and delegates JSON parsing
to Go's standard library. A malformed record returns a contextual error and a
zero `Record`, avoiding use of any partially decoded fields. It does not scan a
file or rebuild the offset index; those are separate functions.

## Tests

Tests cover keyed input with a newline, a null key without a newline, all four
record fields including the numeric millisecond timestamp, and malformed JSON
that includes a partial offset.

## Deliberate differences

The reference code parses records as part of its log operations. This Go project
isolates parsing into one function so the persistence format can be tested
without opening a file. It accepts either form of line termination because
Go's line scanners may remove delimiters before returning a line.
