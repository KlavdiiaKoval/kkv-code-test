# Simple Go project Makefile
# Usage: make <target>
# Run `make help` to list targets.

APP_NAME := kkv-code-test
PKG := ./...
COVER_PROFILE := coverage.out
COVER_PROFILE_ALL := coverage_all.out
GO ?= go

# Colors
YELLOW=\033[33m
CYAN=\033[36m
RESET=\033[0m

.PHONY: help deps tidy build test test-cover cover cover-html race lint clean docker-build-queue docker-build-worker compose-up compose-down

help: ## Show this help
	@echo "Available targets:" && \
	grep -E '^[a-zA-Z0-9_.-]+:.*?##' Makefile | \
	awk 'BEGIN {FS=":"} {printf "  ${CYAN}%-28s${RESET} %s\n", $$1, substr($$0, index($$0,$$3))}'

# Dependency management

deps: ## Download go modules
	$(GO) mod download

tidy: ## Run go mod tidy
	$(GO) mod tidy

# Build

build: ## Build all binaries (if multiple main packages exist)
	$(GO) build $(PKG)

# Testing & Coverage

test: ## Run tests
	$(GO) test $(PKG)

test-cover: ## Run tests with coverage (package aggregate)
	$(GO) test $(PKG) -cover

cover: ## Generate coverage profile (atomic) and show function summary
	$(GO) test $(PKG) -coverprofile $(COVER_PROFILE) -covermode=atomic
	@echo "\nFunction coverage:" && $(GO) tool cover -func=$(COVER_PROFILE) | tail -n +2

cover-html: cover ## Generate HTML coverage report (opens in browser if open is available)
	$(GO) tool cover -html=$(COVER_PROFILE) -o coverage.html
	@if command -v open >/dev/null 2>&1; then open coverage.html; fi

race: ## Run tests with race detector
	$(GO) test -race $(PKG)

# Lint (placeholder – integrate golangci-lint if desired)
lint: ## Run basic vet and fmt checks
	$(GO) vet $(PKG)
	@echo "Checking formatting (no changes should be listed)..." && \
	FMT_OUT=$$(gofmt -l .); if [ -n "$$FMT_OUT" ]; then echo "Files need gofmt:"; echo "$$FMT_OUT"; exit 1; else echo "Formatting OK"; fi

# Cleaning
clean: ## Remove build artifacts and coverage files
	rm -f $(COVER_PROFILE) $(COVER_PROFILE_ALL) coverage.html
	find . -name '*.test' -type f -delete

# Docker / Compose helpers

docker-build-queue: ## Build queue image
	docker build -f Dockerfile.queue -t $(APP_NAME)-queue:latest .

docker-build-worker: ## Build worker image
	docker build -f Dockerfile.worker -t $(APP_NAME)-worker:latest .

compose-up: ## Start services via docker-compose
	docker compose up --build -d

compose-down: ## Stop services and remove containers
	docker compose down

# Convenience meta target
all: tidy deps test-cover ## Tidy, download deps, run coverage tests

.DEFAULT_GOAL := help
