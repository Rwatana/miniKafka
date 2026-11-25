# Registering the HTTP routes

## Kafka concept

The broker exposes distinct produce, consume, commit, metadata, and lag
operations. A route table makes each operation's HTTP method and path explicit.

## Reference source

`broker/server.js` registers `POST /produce`, `GET /consume`, `POST /commit`,
`GET /topics`, and `GET /lag`. The dashboard uses the root page in the Go
process.

## Go design

`httpadapter.NewRouter` accepts already-constructed handlers and registers them
with Go's method-aware `ServeMux` patterns. The exact-root pattern serves the
dashboard without matching unknown paths.

## Tests

Tests verify every method/path mapping, method rejection, and unknown paths.

## Deliberate differences

The reference runs Express and the dashboard separately during development.
This Go router serves the dashboard and API from one process.
