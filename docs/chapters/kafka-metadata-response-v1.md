# Encoding Metadata response v1

## Kafka concept

Metadata v1 extends the v0 response with a controller ID, broker rack fields,
and an `is_internal` flag on each topic. These additions change the byte
layout, so a v0 response cannot be reused for a v1 client.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Metadata API key 3 response v1 with response header v0. Each broker
contains a nullable rack string; each topic includes `is_internal` before its
partition array.

## Go design

`encodeMetadataResponseV1` writes the fixed-width arrays in schema order and
uses a null rack because MiniKafka does not configure broker racks. Its caller
provides the controller ID. User topics are encoded as non-internal.

## Tests

Exact-byte fixtures cover a populated broker/topic/partition response and an
empty response. Invalid UTF-8 and strings larger than the legacy INT16 length
limit are rejected.

## Deliberate differences

This encoder covers Metadata v1 response bytes only. Request validation,
connection dispatch, and API-version advertisement are separate functions.
