# Testing MiniKafka

The default `make ci` target runs documentation checks, Go unit tests, static
analysis, and 100 percent statement coverage. PostgreSQL-specific end-to-end
tests use the `integration` build tag and run in a separate GitHub Actions
workflow with a disposable PostgreSQL service.

To run the same integration suite locally, start PostgreSQL, set
`DATABASE_URL`, and run `make integration`. Without that variable, the
database-backed test skips itself while the other tagged tests still run.
