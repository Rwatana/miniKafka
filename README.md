# MiniKafka

MiniKafka is an educational Go implementation of the core features in
[ayuxh16/Minikafka](https://github.com/ayuxh16/Minikafka/tree/main/minikafka):
append-only topic-partition logs, offset-based reads, consumer-group offset
commits, lag reporting, and a small dashboard. It is being built from scratch
in reviewed, function-sized Issues and one-commit Pull Requests. It is not a
Kafka protocol-compatible broker.

The repository workflow is tracked in [Issue #1](https://github.com/Rwatana/miniKafka/issues/1).
The reference feature map and implementation sequence are documented in the
[architecture](docs/architecture.md) and [roadmap](docs/roadmap.md). Each
implementation step is tracked by its own Issue and one-commit PR.

See the [learning roadmap](docs/roadmap.md), [architecture and development rules](docs/architecture.md),
and [Contributing](CONTRIBUTING.md).

## Development checks

```sh
make ci
```

## Run the broker

The broker requires a reachable PostgreSQL database. The default connection
matches the reference project:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/minikafka \
DATA_DIR=./data PORT=5000 go run ./cmd/minikafka
```

`DATABASE_URL`, `DATA_DIR`, and `PORT` are optional. Startup pings PostgreSQL
and applies `internal/infrastructure/postgres/schema.sql`. The HTTP server
provides `/produce`, `/consume`, `/commit`, `/topics`, `/lag`, and the dashboard
at `/`.

The docs folder explains each function-sized behavior and its relationship to
the reference MiniKafka and Kafka concepts.
