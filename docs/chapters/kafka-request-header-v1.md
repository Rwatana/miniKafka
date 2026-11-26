# Decoding Kafka request header v1

## Kafka concept

Every Kafka request identifies its API and schema version, then carries a
correlation ID so the response can be matched to the request. The non-flexible
request header also includes a nullable client ID before the API-specific body.

## Kafka 4.4.0 source map

The [Kafka 4.4 protocol guide](https://kafka.apache.org/44/design/protocol/)
defines header v1 as API key (INT16), API version (INT16), correlation ID
(INT32), and nullable client ID. ApiVersions request versions 0 through 2 use
this header version. The pinned Java implementation is
[`RequestHeader.java`](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/requests/RequestHeader.java).

## Go design

`DecodeRequestHeaderV1` reads the fixed-width fields in network byte order,
decodes the INT16-length UTF-8 client ID (`-1` means null), and returns the
remaining bytes as the API body. Flexible header v2 and tagged fields have a
separate decoder so each wire schema stays explicit.

## Tests

Fixtures verify known header values and body remainder, null and empty client
IDs, truncation at each field, invalid negative lengths, overlong client IDs,
and malformed UTF-8.

## Deliberate differences

This decoder handles header v1 only. It does not infer a header version from an
API key and API version; that mapping belongs to API dispatch and will be
implemented against the Kafka 4.4.0 API schema table.
