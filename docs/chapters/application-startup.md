# Starting the MiniKafka broker

## Kafka concept

A broker process combines partition storage, offset persistence, network
handlers, and the dashboard into one service that producers and consumers can
use.

## Reference source

`broker/server.js` reads `DATABASE_URL`, creates the PostgreSQL pool, and
starts the HTTP server using `PORT`. The log store reads `DATA_DIR`.

## Go design

`cmd/minikafka/main.go` prepares PostgreSQL, constructs the broker, injects the
database-backed offset operations into the HTTP handlers, registers the
reference routes, and listens on the configured port.

## Tests

The application package is build-checked with `go build ./cmd/minikafka`. Its
individual handlers and route registration have focused tests in their own
function-sized Issues.

## Deliberate differences

The Go process embeds and applies the schema at startup, and serves the small
dashboard from the same HTTP server.
