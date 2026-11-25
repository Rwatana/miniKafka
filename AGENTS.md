# Repository Instructions

## Workflow

- Start each behavior change from a GitHub Issue with explicit acceptance criteria.
- Use `feature/#<issue-number>_<snake_case_description>` for feature branches.
- Prefix Issue and Pull Request titles with a value from `hack/prefix.yml`.
- Put `## Issue Number` and the matching `#<issue-number>` in every Pull Request body.
- Put `Closes #<issue-number>` in the Pull Request body.
- Keep exactly one commit in each Pull Request; amend that commit for revisions.
- Format commit subjects as `[Scope] Summary.` with a non-empty bracketed scope and final period.
- Keep each Pull Request to one production function and its tests and learning note.

## Implementation

- Follow the dependency direction in `docs/architecture.md`.
- Add a failing test before the minimum implementation.
- Use temporary directories for tests that touch the filesystem.
- Run `gofmt`, `go test ./...`, and `go vet ./...` for Go changes.

## Documentation

- Give every exported Go name a complete Go doc comment starting with its name.
- Update the relevant chapter note with the Kafka concept, Java source map, Go design,
  tests, and intentional differences.
- Keep editable diagrams in Draw.io format and explain them in Markdown.
- Do not include generated logs or runtime data in commits.

## Validation

- Run `python3 scripts/check_docs.py` for every change.
- Keep independent GitHub Actions checks in separate descriptive workflow files.
- Require 100% Go statement coverage, including no unexecuted statement blocks.
