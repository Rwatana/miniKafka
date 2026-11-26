# Encoding Kafka Fetch v4 responses

## Kafka concept

Fetch v4 returns each requested partition's error, high watermark, last stable
offset, optional aborted transaction list, and optional record bytes. Kafka
uses nullable arrays and bytes, so null and empty have distinct wire values.

## Go design

`encodeFetchResponseV4` writes all fields in schema order and preserves nil
slices as Kafka null sentinels while encoding non-nil empty slices as zero
lengths. Topic names use Kafka's signed 16-bit STRING length.

## Tests

Fixtures verify multiple topics and partitions, nullable and empty values,
aborted transaction entries, exact big-endian field order, and invalid topic
names.

## Deliberate limits

This step only encodes the body. Log reads, response framing, and dispatch are
separate implementation steps.
