# Reading from a partition offset

## Kafka concept

Consumer reads start at an offset and return records in order. The byte index
lets the broker seek to the first requested record and cap the read to a
bounded number of log lines.

## Reference source

`readFrom` in
[`broker/log.js`](https://github.com/ayuxh16/Minikafka/blob/main/minikafka/broker/log.js)
uses the offset index to seek to the starting byte and reads through a bounded
end position. It skips blank lines in the selected range.

## Go design

`readFromOffset` seeks the owned file to the indexed position, reads at most the
requested number of lines, and decodes each nonblank line with `decodeRecord`.
Invalid offsets, malformed records, truncated indexes, and I/O failures are
reported instead of returning partial results.

## Tests

Tests cover ordered reads, a one-record bound, final records without a newline,
end offsets, zero and negative limits, blank lines, malformed JSON, seek/read
failures, and an index pointing past EOF.

## Deliberate differences

The reference uses a file stream per consume request and defaults the maximum
in the HTTP function. This core function requires an explicit maximum and leaves
that API default to the future HTTP adapter.
