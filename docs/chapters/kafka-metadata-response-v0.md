# Encoding Metadata response v0

## Kafka concept

Kafka Metadata responses tell a client which brokers exist and which broker
leads each partition. Clients use the broker endpoints and replica placement
to route Produce and Fetch requests.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines Metadata API key 3 response v0 as broker and topic arrays. Each topic
contains a partition array with error, index, leader, replica, and ISR fields.
The response uses header version 0.

## Go design

`encodeMetadataResponseV0` encodes legacy INT32 array counts, INT16 errors and
STRING lengths, and INT32 broker/partition identifiers in network byte order.
Its value types model the exact broker, topic, and partition response fields.

## Tests

Exact-byte fixtures cover empty metadata and one broker/topic/partition. Error
fixtures reject invalid UTF-8 and a string longer than the INT16 wire limit.

## Deliberate differences

This encodes the response body only. The response header/frame, metadata
discovery, and mapping from on-disk MiniKafka partitions remain separate work.
