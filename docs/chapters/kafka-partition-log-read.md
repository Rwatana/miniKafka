# Reading Kafka batches from a partition log

## Kafka concept

Fetch returns complete record batches from the requested offset and reports
the partition high watermark. Brokers keep batch boundaries intact even when a
client's byte limit is smaller than the first available batch.

## Go design

`ReadKafkaRecordBatches` scans and validates each binary batch, returns batches
whose offset range reaches the requested offset, and calculates the high
watermark across the entire log. It applies the byte limit between batches and
still returns the first matching batch when that batch exceeds the limit.

## Tests

Fixtures cover offset selection, high watermark, byte limits, empty logs,
corrupt batch data, closed handles, and read errors.

## Deliberate limits

This function reads one already-open partition log. Broker lookup and Fetch
response assembly remain separate steps.
