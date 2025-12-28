# juno-scan

Watch-only scanner/indexer for Juno Cash shielded deposits (Orchard).

Designed for exchange integrations where recipient data/value are encrypted on-chain:

- scans blocks using UFVKs (trial-decrypt), not plaintext address matching
- tracks notes + nullifiers (shielded “UTXO set”) per `wallet_id`
- emits deposit/spend events and exposes balances for tx building

Status: work in progress.

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
