# Encoding Fetch v2 responses

## Kafka concept

Fetch v2 returns a throttle time followed by topic and partition arrays. Each
partition has its error, high watermark, and nullable record set. Later Fetch
versions add fields such as the last stable offset and aborted transactions.

## Go design

`encodeFetchResponseV2` writes the v2 field order and preserves the distinction
between a null record set (`nil`) and an empty record set (zero bytes).

## Tests

Expected bytes cover populated partitions, an error partition, null and empty
record sets, and invalid topic names.

## Deliberate limits

This step encodes a response body only. Request handling and API dispatch are
separate steps.
