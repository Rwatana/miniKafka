# Contributing

MiniKafka is built in small, reviewable learning steps. Each feature starts with an
Issue that states one observable behavior and its acceptance criteria. A Pull
Request implements exactly one production function, adds its tests, and updates the
chapter note that explains the behavior. Keep one commit per Pull Request.

Commit subjects use `[Scope] Summary.` with a non-empty bracketed scope and a
final period. Separate CI checks validate the scope and period on commits in the
Pull Request only.

Use branch names such as `feature/#42_encode_frame` and titles such as
`[Go] Encode one bounded log frame`. The Pull Request body must include the Issue
number and `Closes #42`.

For Go changes, follow test-first development and run:

```sh
gofmt -w <changed-go-files>
go test ./...
go vet ./...
python3 scripts/check_docs.py
```

The coverage workflow also requires every Go statement to execute and total
statement coverage to equal 100.0%.
