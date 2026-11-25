# Running the broker and PostgreSQL together

## Kafka concept

The broker writes records to partition log files and uses PostgreSQL for
consumer-group offsets. A local stack needs both durable stores available
before the HTTP service starts.

## Reference source

The reference server reads `DATABASE_URL`, `DATA_DIR`, and `PORT` from its
environment. The Go Compose stack supplies the same settings.

## Go design

`compose.yaml` starts PostgreSQL with a health check, then starts the Go broker
after the database becomes ready. PostgreSQL data and partition logs use
separate named volumes. The multi-stage Dockerfile compiles the Go command into
a small non-root runtime image.

## Tests

The compose file can be parsed with `docker compose config`. The integration
workflow independently validates the broker against PostgreSQL in CI.

## Deliberate differences

The reference project uses a Node.js development setup. The Go repository
builds a single broker image and runs the dashboard from the same process.
