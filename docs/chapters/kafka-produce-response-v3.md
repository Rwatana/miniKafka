# Encoding Produce v3 responses

## Kafka concept

Produce v3 returns a response for each requested topic and partition. Each
partition result contains its error code, base offset, and log append time;
the response ends with a throttle duration. The schema uses legacy
length-prefixed strings and arrays.

## Go design

`encodeProduceResponseV3` writes the schema in field order using big-endian
integers. It validates topic names as UTF-8 strings representable by Kafka's
signed 16-bit string length.

## Tests

Fixtures assert the bytes for multiple topics and partitions, an empty
response, invalid UTF-8, and an overlong topic name.

## Deliberate limits

This function only encodes the response body. Request handling, append results,
response headers, and frame-size enforcement remain separate steps.
