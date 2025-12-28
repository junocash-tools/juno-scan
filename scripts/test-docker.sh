#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -f docker-compose.test.yml)

cleanup() {
  "${compose[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d --build

export JUNO_TEST_RPC_URL="http://127.0.0.1:18232"
export JUNO_TEST_RPC_USER="rpcuser"
export JUNO_TEST_RPC_PASS="rpcpass"
export JUNO_TEST_JUNOCASHD_CONTAINER="junoscan-junocashd"
export JUNO_TEST_JUNOCASHD_DATADIR="/data"
export JUNO_TEST_JUNOCASHD_RPC_PORT="8232"
export JUNO_TEST_DOCKER="1"
export JUNO_TEST_NATS_URL="nats://127.0.0.1:14222"
export JUNO_TEST_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:25672/"
export JUNO_TEST_KAFKA_BROKERS="127.0.0.1:19092"

make test-integration-docker
make test-e2e
