# Accepting Fetch-capable Kafka clients

## Kafka concept

A Kafka broker accepts many client TCP connections. Each connection has its own
ordered request stream, while independent clients are served concurrently.

## Go design

`ServeKafkaWithFetchReader` accepts a connection, starts the reader-aware
connection loop in a goroutine, and closes the socket when that loop exits. It
passes both the Fetch batch reader and the optional ListOffsets lookup callback
to each connection. It keeps the older listener function available for clients
that only use the previous API set.

## Tests

An actual local TCP client issues Fetch v4 and ListOffsets v1 and observes
responses from both broker callbacks. Tests also cover accept errors and clean
shutdown after listener closure.

## Deliberate limits

The application startup still needs to choose this listener and pass the
broker read method. ApiVersions must advertise Fetch v4 before standard clients
will discover it.
