# Reading a Kafka request frame

## Kafka concept

Kafka's TCP protocol uses length-delimited request/response messages. A server
reads the signed 32-bit message size first, then reads exactly that many bytes
before parsing the request header and API body.

## Kafka 4.4.0 source map

The framing is specified by the [Kafka protocol guide](https://kafka.apache.org/protocol.html)
and implemented in the pinned [Kafka 4.4.0 protocol source](https://github.com/apache/kafka/tree/4.4.0/clients/src/main/java/org/apache/kafka/common/protocol).

## Go design

`ReadRequestFrame` uses `io.ReadFull` for both the four-byte big-endian length
prefix and the declared body. The caller supplies a positive maximum size so a
malformed peer cannot force an unbounded allocation. It returns `io.EOF` only
at a clean frame boundary; partial prefixes and bodies are reported as
unexpected EOF. API key, version, correlation ID, and flexible-header parsing
remain separate protocol steps.

## Tests

Tests read consecutive frames, reject invalid limits and sizes before reading
body bytes, and check clean EOF plus truncated prefix/body behavior.

## Deliberate differences

The existing educational MiniKafka uses HTTP/JSON. This package starts the
separate Apache Kafka binary protocol and targets Kafka 4.4.0; it is not yet a
complete Kafka-compatible server.
