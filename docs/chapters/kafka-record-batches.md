# Decoding Kafka record sets

## Kafka concept

A Produce partition's record-set field can contain length-delimited record
batches. Each batch has its own fixed header, magic value, checksum, and record
payload. The broker must advance by each declared batch length rather than
treat the whole field as one batch.

## Go design

`decodeRecordBatchesV2` walks the record-set bytes in order. It checks each
batch's prefix and declared size, then delegates magic-v2 header and CRC32C
validation to `decodeRecordBatchV2`. A malformed or truncated batch rejects the
whole record set.

## Tests

Fixtures cover two ordered batches, an empty set, truncated prefixes and
payloads, invalid lengths, a bad CRC, and trailing incomplete bytes.

## Deliberate limits

This step only decodes batch framing. Compression, record semantics, broker
append behavior, and Produce responses are separate protocol features.
