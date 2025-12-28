# juno-scan

Watch-only scanner/indexer for Juno Cash shielded deposits (Orchard).

Designed for exchange integrations where recipient data/value are encrypted on-chain:

- scans blocks using UFVKs (trial-decrypt), not plaintext address matching
- tracks notes + nullifiers (shielded “UTXO set”) per `wallet_id`
- emits deposit/spend events and exposes balances for tx building

Status: work in progress.

## Events

Events are available via `GET /v1/wallets/{wallet_id}/events`.

Two deposit-related events are emitted:

- `DepositEvent`: a note was detected in a scanned block.
- `DepositConfirmed`: the note has reached the configured confirmation threshold.

The confirmation threshold defaults to `100` and can be configured via `-confirmations` / `JUNO_SCAN_CONFIRMATIONS`.

## Run

Requires a running `junocashd` (RPC) and a storage backend.

### Postgres (default)

`juno-scan -db-driver postgres -db-dsn <dsn> -rpc-url <url> -rpc-user <user> -rpc-pass <pass> -ua-hrp <j|jregtest>`

Optional: `-db-schema <schema>` to set Postgres `search_path`.

### Embedded RocksDB (Pebble)

`juno-scan -db-driver rocksdb -db-path ./data/juno-scan.db -rpc-url <url> -rpc-user <user> -rpc-pass <pass> -ua-hrp <j|jregtest>`

### MySQL

MySQL support is behind a build tag:

`go build -tags=mysql -o bin/juno-scan ./cmd/juno-scan`

Then run with `-db-driver mysql -db-dsn <dsn>`.

## Broker (optional)

`juno-scan` can publish wallet events to a message broker using an outbox loop (events are always stored in the DB first).

Configure:

- `-broker-driver` / `JUNO_SCAN_BROKER_DRIVER`: `none` (default), `kafka`, `nats`, `rabbitmq`
- `-broker-url` / `JUNO_SCAN_BROKER_URL`
- `-broker-topic` / `JUNO_SCAN_BROKER_TOPIC` (Kafka topic / NATS subject / RabbitMQ queue name)

Broker adapters are behind build tags:

- Kafka: `-tags=kafka`
- NATS: `-tags=nats`
- RabbitMQ: `-tags=rabbitmq`
