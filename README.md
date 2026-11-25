# MiniKafka

MiniKafka is an educational Go implementation of the core features in
[ayuxh16/Minikafka](https://github.com/ayuxh16/Minikafka/tree/main/minikafka):
append-only topic-partition logs, offset-based reads, consumer-group offset
commits, lag reporting, and a small dashboard. It is being built from scratch
in reviewed, function-sized Issues and one-commit Pull Requests. It is not a
Kafka protocol-compatible broker.

The repository bootstrap is tracked in [Issue #1](https://github.com/Rwatana/miniKafka/issues/1).
The reference feature map and implementation sequence are documented in the
[architecture](docs/architecture.md) and [roadmap](docs/roadmap.md). The first
broker behavior will be implemented in its own Issue and PR after the repository
workflow and scope documentation have been reviewed.

See the [learning roadmap](docs/roadmap.md), [architecture and development rules](docs/architecture.md),
and [Contributing](CONTRIBUTING.md).

## Development checks

```sh
make ci
```

The docs folder explains each function-sized behavior and its relationship to
the reference MiniKafka and Kafka concepts.
