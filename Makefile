.PHONY: build rust-build rust-test test test-unit test-integration test-integration-docker test-e2e test-e2e-docker fmt tidy clean docker-up docker-down test-docker

BIN_DIR := bin
BIN := $(BIN_DIR)/juno-scan

RUST_MANIFEST := rust/scan/Cargo.toml
DOCKER_COMPOSE := docker compose -f docker-compose.test.yml

TESTFLAGS ?=

ifneq ($(JUNO_TEST_LOG),)
TESTFLAGS += -v
endif

GOCACHE ?= $(CURDIR)/.cache/go-build
GO_TEST_TIMEOUT ?= 20m
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

test-integration-docker: rust-build
	GOCACHE=$(GOCACHE) go test $(TESTFLAGS) -timeout=$(GO_TEST_TIMEOUT) -tags=integration,docker,kafka,nats,rabbitmq,mysql ./...

test-e2e: build
	GOCACHE=$(GOCACHE) go test $(TESTFLAGS) -tags=e2e ./...

test-e2e-docker: build
	GOCACHE=$(GOCACHE) go test $(TESTFLAGS) -timeout=$(GO_TEST_TIMEOUT) -tags=e2e,docker ./...

test: rust-test test-unit test-integration test-e2e

docker-up:
	$(DOCKER_COMPOSE) up -d --build

docker-down:
	$(DOCKER_COMPOSE) down -v

test-docker:
	$(MAKE) test-integration-docker
	$(MAKE) test-e2e-docker

fmt:
	gofmt -w .

tidy:
	GOCACHE=$(GOCACHE) go mod tidy

clean:
	rm -rf $(BIN_DIR)
	rm -rf rust/scan/target
