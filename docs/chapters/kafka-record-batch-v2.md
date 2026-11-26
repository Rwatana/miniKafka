# Decoding Kafka record batch v2 headers

## Kafka concept

Modern Kafka producers send records in batches. The batch header identifies
the offset and timestamp range, producer sequence state, compression mode,
and record count before the records are parsed.

## Kafka 4.4.0 reference

The official [Kafka 4.4 Message Format](https://kafka.apache.org/44/implementation/message-format/)
defines the magic-v2 fixed header and states that the batch length excludes
the first 12 bytes. CRC-32C (Castagnoli) covers the bytes from attributes
through the end of the batch; the leader epoch is outside the checksum.

## Go design

`decodeRecordBatchV2` validates the exact batch size, magic, CRC32C, and
non-negative record count, then exposes the typed header fields and leaves the
record bytes opaque for the next parser step. Compressed payloads are accepted
at this stage without decompression.

## Tests

Fixtures cover a populated batch, empty record payload, opaque compressed
payload, truncated headers, invalid lengths, wrong magic, corrupt CRC, and a
negative count.

## Deliberate limits

This does not decompress records or validate their contents. Produce handling
will reject unsupported compression until the corresponding codecs are
implemented.
