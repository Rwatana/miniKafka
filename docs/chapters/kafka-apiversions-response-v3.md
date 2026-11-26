# Encoding ApiVersions response v3

## Kafka concept

Kafka clients request the supported API version ranges during connection
setup. ApiVersions v3 uses a flexible response body so its API range array is
compact and the response ends with a tagged-fields section.

## Kafka 4.4.0 reference

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines the v3 response fields in this order: INT16 error code, compact array
of API key/minimum/maximum triples, INT32 throttle time, and tagged fields.
Its response header version remains v0.

## Go design

`encodeAPIVersionsResponseV3` writes the error code and API ranges in network
byte order, encodes compact array length as unsigned varint length-plus-one,
and emits zero throttle time plus an empty tag section. Callers supply the
supported API ranges.

## Tests

Fixtures assert the exact bytes for multiple API ranges and an empty list with
an error code.

## Deliberate differences

This function encodes the response body only. The response header and outer
size frame are separate codec functions. Optional tagged feature payloads are
not emitted by this initial subset.
