# Decoding Kafka magic-v2 records

## Kafka concept

Record batches contain individually length-delimited records. Each record
stores timestamp and offset deltas, nullable key/value byte arrays, and
ordered application headers.

## Kafka 4.4.0 reference

The [Kafka 4.4 Message Format](https://kafka.apache.org/44/implementation/message-format/)
defines record fields as signed zig-zag VARINT/VARLONG values and represents
header keys as non-null UTF-8 strings with nullable byte-array values.

## Go design

`decodeRecordV2` parses one length-delimited record, preserving key and value
payloads as bytes and returning the consumed size for the next record. It
validates nested field bounds, header count limits, and header-key UTF-8.

## Tests

Fixtures cover signed deltas, null and populated key/value fields, multiple
headers, following record bytes, malformed varints and lengths, invalid
header keys, excessive counts, and trailing bytes.

## Deliberate limits

This function parses uncompressed record bytes only. Batch decompression and
conversion of Kafka records into the MiniKafka storage model are later steps.
