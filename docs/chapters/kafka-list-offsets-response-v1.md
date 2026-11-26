# Encoding ListOffsets response v1

## Kafka concept

ListOffsets returns one offset result for each requested topic partition. The
response must preserve request ordering and include the partition error code,
timestamp, and offset.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines ListOffsets v1 response as a fixed-width topics array. Each partition
result contains `partition_index`, `error_code`, `timestamp`, and `offset`;
the response uses header v0.

## Go design

`encodeListOffsetsResponseV1` serializes the provided ordered topic and
partition results in network byte order. It validates topic names and returns
only the response body; the request handler adds the correlation header and
outer size prefix.

## Tests

Exact-byte fixtures cover a populated response, signed timestamp values, and
an empty response. Invalid UTF-8 and names too long for legacy STRING are
rejected.

## Deliberate differences

This function only encodes ListOffsets v1 response bodies. Resolving offsets
and framing a response are separate functions.
