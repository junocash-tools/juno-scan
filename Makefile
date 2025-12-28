.PHONY: build rust-build rust-test test test-unit test-integration test-e2e fmt tidy clean docker-up docker-down test-docker

BIN_DIR := bin
BIN := $(BIN_DIR)/juno-scan

RUST_MANIFEST := rust/scan/Cargo.toml
DOCKER_COMPOSE := docker compose -f docker-compose.test.yml

TESTFLAGS ?=

ifneq ($(JUNO_TEST_LOG),)
TESTFLAGS += -v
endif

GOCACHE ?= $(CURDIR)/.cache/go-build

build: rust-build
	@mkdir -p $(BIN_DIR)
	GOCACHE=$(GOCACHE) go build -o $(BIN) ./cmd/juno-scan

rust-build:
	cargo build --release --manifest-path $(RUST_MANIFEST)

rust-test:
	cargo test --manifest-path $(RUST_MANIFEST)

test-unit:
	GOCACHE=$(GOCACHE) CGO_ENABLED=0 go test $(TESTFLAGS) ./...

test-integration: rust-build
	GOCACHE=$(GOCACHE) go test $(TESTFLAGS) -tags=integration ./...

test-e2e: build
	GOCACHE=$(GOCACHE) go test $(TESTFLAGS) -tags=e2e ./...

test: rust-test test-unit test-integration test-e2e

docker-up:
	$(DOCKER_COMPOSE) up -d --build

docker-down:
	$(DOCKER_COMPOSE) down -v

test-docker:
	./scripts/test-docker.sh

fmt:
	gofmt -w .

tidy:
	GOCACHE=$(GOCACHE) go mod tidy

clean:
	rm -rf $(BIN_DIR)
	rm -rf rust/scan/target
