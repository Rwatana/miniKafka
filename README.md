# MiniKafka

MiniKafka is an educational Kafka-like system implemented incrementally in Go.
Development follows the reference repository's append-only partition log,
offset-based reads, and consumer offset tracking, with each behavior isolated
in an Issue and a one-commit Pull Request.

The repository bootstrap is tracked in [Issue #1](https://github.com/Rwatana/miniKafka/issues/1).
The first broker behavior will be implemented in a separate Issue after this
development workflow is reviewed.

See the [learning roadmap](docs/roadmap.md), [architecture and development rules](docs/architecture.md),
and [Contributing](CONTRIBUTING.md).

## Development checks

```sh
make ci
```

The docs folder explains each function-sized behavior and its relationship to
the reference MiniKafka and Kafka concepts.
