# juno-scan

Watch-only scanner/indexer for Juno Cash shielded deposits (Orchard).

Designed for exchange integrations where recipient data/value are encrypted on-chain:

- scans blocks using UFVKs (trial-decrypt), not plaintext address matching
- tracks notes + nullifiers (shielded “UTXO set”) per `wallet_id`
- emits deposit/spend events and exposes balances for tx building

Status: work in progress.
