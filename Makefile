GO ?= go
PYTHON ?= python3
COVERAGE_FILE ?= coverage.out
COVERAGE_CACHE ?= /tmp/minikafka-coverage-cache

.DEFAULT_GOAL := help
.PHONY: help test integration coverage coverage-check coverage-report coverage-html vet docs test-commit-prefix test-commit-period ci

help: ## Show available development checks
	@awk 'BEGIN { FS = ":.*##" } /^[a-zA-Z0-9_-]+:.*##/ { printf "%-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

test: ## Run all Go tests
	$(GO) test ./...

integration: ## Run PostgreSQL-backed integration tests (requires DATABASE_URL)
	$(GO) test -tags=integration ./...

coverage: ## Run all Go tests and write a statement coverage profile
	GOCACHE=$(COVERAGE_CACHE) $(GO) test ./... -coverprofile=$(COVERAGE_FILE)

coverage-check: coverage ## Require full-module statement coverage to be 100.0 percent
	@coverage="$$($(GO) tool cover -func=$(COVERAGE_FILE) | awk '$$1 == "total:" { print $$3 }')"; \
	if [ -z "$$coverage" ]; then echo "Could not read total statement coverage."; exit 1; fi; \
	echo "Go statement coverage: $$coverage"; \
	if [ "$$coverage" != "100.0%" ]; then echo "Coverage must be 100.0%."; exit 1; fi

coverage-report: ## Show per-function coverage from the existing profile
	@test -f $(COVERAGE_FILE) || { echo "Missing $(COVERAGE_FILE); run 'make coverage' first."; exit 1; }
	$(GO) tool cover -func=$(COVERAGE_FILE)

coverage-html: coverage ## Generate an HTML coverage report
	$(GO) tool cover -html=$(COVERAGE_FILE) -o coverage.html

vet: ## Run Go static analysis
	$(GO) vet ./...

docs: ## Validate repository documentation
	$(PYTHON) scripts/check_docs.py

test-commit-prefix: ## Test the commit subject prefix rule
	bash scripts/test_commit_subject_prefix.sh

test-commit-period: ## Test the commit subject final period rule
	bash scripts/test_commit_subject_period.sh

ci: docs test-commit-prefix test-commit-period ## Run all checks available at the current repository stage
	@if [ -f go.mod ]; then \
		$(MAKE) test coverage-check vet; \
	else \
		echo "Go checks are deferred until the first Go module Issue."; \
	fi
