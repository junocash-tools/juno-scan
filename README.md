# juno-scan

Watch-only scanner/indexer for Juno Cash Orchard activity.

Designed for exchange/custody integrations where recipient/value data are encrypted on-chain:

- scans blocks using UFVKs (trial decryption), not plaintext address matching
- tracks notes + nullifiers (shielded “UTXO set”) per `wallet_id`
- emits deposit/spend events (including confirmation + reorg lifecycle)
- exposes an HTTP API for wallets, notes, events, and Orchard witnesses
- optionally publishes events to a broker (Kafka/NATS/RabbitMQ) via an outbox loop

## Requirements

- `junocashd` RPC access (recommended: run with `-txindex=1`)
- Storage backend (Postgres, RocksDB/Pebble, optional MySQL)
- Go 1.24+ and Rust (build/runtime uses CGO + the Rust `juno_scan` library)

## Quickstart

Build the scanner binary (builds the Rust library first):

- `make build` (outputs `bin/juno-scan`)

Run with Postgres:

- `bin/juno-scan -rpc-url http://127.0.0.1:8232 -rpc-user <user> -rpc-pass <pass> -db-dsn <postgres-dsn> -ua-hrp <j|jregtest>`

Register wallets (UFVKs) before scanning if you need full history:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/wallets \
  -H 'content-type: application/json' \
  -d '{"wallet_id":"exchange-hot-001","ufvk":"<ufvk>"}'
```

Wallets added after the scanner has already advanced past a height are only detected going forward; to backfill, restart from a fresh database.

Fetch events / notes:

```sh
curl -sS "http://127.0.0.1:8080/v1/wallets/exchange-hot-001/events"
curl -sS "http://127.0.0.1:8080/v1/wallets/exchange-hot-001/notes"           # unspent only (default)
curl -sS "http://127.0.0.1:8080/v1/wallets/exchange-hot-001/notes?spent=true" # include spent
```

## Configuration

All configuration can be provided via flags or environment variables.
Durations use Go’s `time.ParseDuration` format (e.g. `500ms`, `2s`, `1m`).

| Flag | Env var | Default | Notes |
| --- | --- | --- | --- |
| `-listen` | `JUNO_SCAN_LISTEN` | `127.0.0.1:8080` | HTTP listen address |
| `-rpc-url` | `JUNO_SCAN_RPC_URL` | `http://127.0.0.1:8232` | `junocashd` RPC URL |
| `-rpc-user` | `JUNO_SCAN_RPC_USER` | _(empty)_ | `junocashd` RPC username |
| `-rpc-pass` | `JUNO_SCAN_RPC_PASS` | _(empty)_ | `junocashd` RPC password |
| `-ua-hrp` | `JUNO_SCAN_UA_HRP` | `j` | Unified Address HRP (`j`, `jregtest`, …) |
| `-poll-interval` | `JUNO_SCAN_POLL_INTERVAL` | `2s` | Used when ZMQ is not enabled |
| `-zmq-hashblock` | `JUNO_SCAN_ZMQ_HASHBLOCK` | _(empty)_ | `tcp://host:port` to receive `hashblock` notifications |
| `-confirmations` | `JUNO_SCAN_CONFIRMATIONS` | `100` | Confirmations required for `*Confirmed` events |
| `-db-driver` | `JUNO_SCAN_DB_DRIVER` | `postgres` | `postgres`, `rocksdb`, `mysql` |
| `-db-dsn` | `JUNO_SCAN_DB_DSN` | _(empty)_ | DSN for Postgres/MySQL (preferred over `-db-url`) |
| `-db-url` | `JUNO_SCAN_DB_URL` | `postgres://localhost:5432/junoscan?sslmode=disable` | Deprecated alias for `-db-dsn` |
| `-db-schema` | `JUNO_SCAN_DB_SCHEMA` | _(empty)_ | Postgres schema; sets `search_path` (optional) |
| `-db-path` | `JUNO_SCAN_DB_PATH` | _(empty)_ | Required when `-db-driver=rocksdb` |
| `-broker-driver` | `JUNO_SCAN_BROKER_DRIVER` | `none` | `none`, `kafka`, `nats`, `rabbitmq` |
| `-broker-url` | `JUNO_SCAN_BROKER_URL` | _(empty)_ | Required when `-broker-driver != none` |
| `-broker-topic` | `JUNO_SCAN_BROKER_TOPIC` | `juno.scan.events` | Kafka topic / NATS subject / RabbitMQ queue |
| `-broker-poll-interval` | `JUNO_SCAN_BROKER_POLL_INTERVAL` | `500ms` | Outbox polling interval |
| `-broker-batch-size` | `JUNO_SCAN_BROKER_BATCH_SIZE` | `1000` | Outbox max events per batch (max 5000) |

## Block notifications (ZMQ)

By default `juno-scan` polls `junocashd` for new blocks using `-poll-interval`.

To reduce scan latency, you can configure `junocashd` to publish ZMQ block notifications and point `juno-scan` at it:

- `junocashd -zmqpubhashblock=tcp://0.0.0.0:28332 ...`
- `bin/juno-scan -zmq-hashblock tcp://127.0.0.1:28332 ...`

When `-zmq-hashblock` is set, `juno-scan` triggers scans from `hashblock` notifications.

## Storage adapters

### Postgres (default)

- Driver: `postgres` (built-in)
- Configure with `-db-dsn` and optional `-db-schema`.
- On startup, `juno-scan` runs migrations automatically.

Example:

- `bin/juno-scan -db-driver postgres -db-dsn "postgres://user:pass@127.0.0.1:5432/junoscan?sslmode=disable" ...`

### Embedded RocksDB (Pebble)

- Driver: `rocksdb` (built-in, backed by Pebble)
- Configure with `-db-path` (a directory path)

Example:

- `bin/juno-scan -db-driver rocksdb -db-path ./data/juno-scan.db ...`

### MySQL

MySQL support is behind build tags.

Build:

- `go build -tags=mysql -o bin/juno-scan ./cmd/juno-scan`

Run:

- `bin/juno-scan -db-driver mysql -db-dsn "<mysql-dsn>" ...`

## Broker publishing (optional)

`juno-scan` can publish wallet events to a broker using an outbox loop:

- events are always written to storage first
- the publisher advances a per-wallet cursor only after successful publish
- delivery is at-least-once (consumers should handle duplicates)

### Broker drivers

Broker drivers are behind build tags; if you enable a driver without building it in, startup fails with a clear error.

Build examples:

- Kafka: `go build -tags=kafka -o bin/juno-scan ./cmd/juno-scan`
- NATS: `go build -tags=nats -o bin/juno-scan ./cmd/juno-scan`
- RabbitMQ: `go build -tags=rabbitmq -o bin/juno-scan ./cmd/juno-scan`
- Multiple: `go build -tags='kafka,nats,rabbitmq,mysql' -o bin/juno-scan ./cmd/juno-scan`

URL formats:

- Kafka: `-broker-url "127.0.0.1:9092,127.0.0.1:9093"` (comma-separated brokers)
- NATS: `-broker-url "nats://127.0.0.1:4222"`
- RabbitMQ: `-broker-url "amqp://user:pass@127.0.0.1:5672/"`

Published message format:

```json
{
  "version": "v1",
  "kind": "DepositConfirmed",
  "wallet_id": "exchange-hot-001",
  "height": 123,
  "payload": { "...": "..." }
}
```

The broker message key is derived from `payload.txid` when present (falls back to `wallet_id`).

## HTTP API

- `GET /v1/health` → scanner status + scanned tip (if any)
- `GET /v1/wallets` → list wallets
- `POST /v1/wallets` → upsert wallet `{wallet_id, ufvk}`
- `GET /v1/wallets/{wallet_id}/events?cursor=<id>&limit=<n>` → wallet event stream (default limit: 100, max: 1000)
- `GET /v1/wallets/{wallet_id}/notes[?spent=true]` → unspent notes (default) or all notes
- `POST /v1/orchard/witness` → compute Orchard witnesses for commitment positions

HTTP API stability:

- Endpoints are versioned under `/v1`. Breaking changes must be introduced under a new path version.
- OpenAPI: `api/openapi.yaml`
- Error responses are currently `text/plain` via `http.Error` (status code + message). Treat non-2xx as failure; do not parse error bodies as JSON.

`/v1/orchard/witness` request:

```json
{
  "anchor_height": 123,
  "positions": [0, 5, 10]
}
```

If `anchor_height` is omitted, the current scanned tip height is used.

## Events

Events are available via `GET /v1/wallets/{wallet_id}/events` (and optionally via the broker outbox).
Payload shapes are documented in `docs/events.md`.

Deposit lifecycle:

- `DepositEvent`: a note was detected in a scanned block.
- `DepositConfirmed`: the note reached the configured confirmation threshold.
- `DepositOrphaned`: the original block was orphaned due to a reorg.
- `DepositUnconfirmed`: a previously-confirmed deposit fell below the confirmation threshold after rollback.

Spend lifecycle:

- `SpendEvent`
- `SpendConfirmed`
- `SpendOrphaned`
- `SpendUnconfirmed`

The confirmation threshold defaults to `100` and can be configured via `-confirmations` / `JUNO_SCAN_CONFIRMATIONS`.

## Development

Prereqs:

- Go 1.24+
- Rust (for `rust/scan`)
- Docker (for `make test-docker`)

Commands:

- Unit tests: `make test-unit`
- Full local suite (Rust + unit + integration + e2e): `make test`
- Docker suite (regtest + DBs + brokers): `make test-docker`
