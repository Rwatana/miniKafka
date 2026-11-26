# Checking Kafka client interoperability

## Kafka concept

Protocol compatibility is most useful when exercised by a client that performs
the real bootstrap sequence: ApiVersions, Metadata, Produce, offset discovery,
and Fetch. A response that matches a hand-written fixture alone can still fail
to satisfy a client library's ordering or defaults.

## Go design

`TestKafkaGoClientProducesAndFetchesThroughKafka` starts the TCP broker
with durable broker callbacks, writes one keyed message through `kafka-go`,
then reads it back through the client's Reader. The broker advertises only the
versions this implementation currently handles, including ListOffsets v1 and
the Fetch v2 schema used by kafka-go's direct partition reader.

## Tests

The integration test verifies that the key and value written by the standard
client are returned from the same topic partition at offset zero.

## Deliberate limits

This test proves interoperability for this Produce/Metadata/ListOffsets/Fetch
path only. It does not establish full Apache Kafka compatibility or cover
transactions, compression, groups, replication, or other protocol versions.
