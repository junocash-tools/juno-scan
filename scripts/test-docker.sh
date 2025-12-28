#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -f docker-compose.test.yml)

cleanup() {
  "${compose[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d --build

wait_port() {
  local port="$1"
  local name="$2"

  if ! command -v nc >/dev/null 2>&1; then
    return 0
  fi

  for _ in {1..60}; do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "timeout waiting for ${name} on port ${port}" >&2
  exit 1
}

wait_port 18232 "junocashd rpc"
wait_port 28332 "junocashd zmq"
wait_port 15432 "postgres"
wait_port 13306 "mysql"
wait_port 14222 "nats"
wait_port 25672 "rabbitmq"
wait_port 19092 "kafka"

export JUNO_TEST_RPC_URL="http://127.0.0.1:18232"
export JUNO_TEST_RPC_USER="rpcuser"
export JUNO_TEST_RPC_PASS="rpcpass"
export JUNO_TEST_JUNOCASHD_CONTAINER="junoscan-junocashd"
export JUNO_TEST_JUNOCASHD_DATADIR="/data"
export JUNO_TEST_JUNOCASHD_RPC_PORT="8232"
export JUNO_TEST_ZMQ_HASHBLOCK="tcp://127.0.0.1:28332"
export JUNO_TEST_DOCKER="1"
export JUNO_TEST_NATS_URL="nats://127.0.0.1:14222"
export JUNO_TEST_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:25672/"
export JUNO_TEST_KAFKA_BROKERS="127.0.0.1:19092"
export JUNO_TEST_POSTGRES_DSN="postgres://junoscan:junoscan@127.0.0.1:15432/junoscan?sslmode=disable"
export JUNO_TEST_MYSQL_ROOT_DSN="root:root@tcp(127.0.0.1:13306)/mysql"

make test-integration-docker
make test-e2e
