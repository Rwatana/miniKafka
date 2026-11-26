# Decoding records from uncompressed Kafka batches

## Kafka concept

A magic-v2 record batch declares its record count and last offset delta. The
broker validates each record and its offset before appending the batch to a
partition log.

## Go design

`decodeUncompressedRecordBatchV2` rejects unsupported compression, decodes
exactly the declared number of records, and checks each offset delta is
non-negative, increasing, and within the batch's last offset delta. It rejects
missing records and trailing bytes.

## Tests

Fixtures cover multiple records, an empty batch, unsupported compression,
negative/excessive counts, malformed records, invalid offsets, and extra bytes.

## Deliberate limits

Compressed record sets need codec support before Kafka clients using gzip,
snappy, LZ4, or Zstandard can produce successfully. This step accepts only
uncompressed batches.
