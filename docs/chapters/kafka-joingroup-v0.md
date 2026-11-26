# Decoding JoinGroup v0 requests

## Kafka concept

Before a consumer can receive partition assignments, it sends JoinGroup to its
group coordinator. The request carries a group ID, member ID, session timeout,
protocol type, and each protocol the member can use with its metadata.

## Go design

`decodeJoinGroupRequestV0` parses the non-flexible v0 body. Kafka strings use a
signed 16-bit byte length, protocol arrays use a signed 32-bit count, and each
protocol's opaque metadata uses a signed 32-bit byte length. The decoder checks
each length against the remaining frame before slicing and rejects malformed
UTF-8 strings and trailing bytes.

## Tests

Tests cover a complete consumer protocol and malformed lengths, counts, UTF-8,
timeouts, and trailing bytes.

## Deliberate limits

This step only decodes the request body. It does not assign members, retain
group state, encode a response, or dispatch JoinGroup on the network yet.
