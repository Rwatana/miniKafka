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

To start the broker and its PostgreSQL database together with Docker Compose:

```sh
docker compose up --build
```

The broker is available at `http://localhost:5000`; the dashboard is at `/`.
Compose keeps PostgreSQL data and partition logs in separate named volumes.
Stop the services with `docker compose down`; remove persistent database and
log data with `docker compose down -v`. To start the Go process directly, set
`DATABASE_URL` to a reachable PostgreSQL database; `DATA_DIR` and `PORT` are
optional. Startup pings PostgreSQL and applies
`internal/infrastructure/postgres/schema.sql`. The HTTP server provides
`/produce`, `/consume`, `/commit`, `/topics`, and `/lag`.

The docs folder explains each function-sized behavior and its relationship to
the reference MiniKafka and Kafka concepts.

The PostgreSQL-backed end-to-end suite runs in CI. To run it locally, start a
PostgreSQL database, set `DATABASE_URL`, and use:

```sh
make integration
```
