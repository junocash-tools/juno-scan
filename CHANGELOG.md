# Changelog

## v1.5.0 (2026-07-21)

- Add durable wallet identity, birthday, backfill progress, address-balance, outgoing-output, subtree-root, and recovery contracts for exchange operation.
- Fence backfill against canonical-chain races, preserve contiguous generation-guarded progress, and recover multi-wallet outgoing transactions correctly.
- Harden event cursors, restart/reset epochs, live-tip readiness, RocksDB recovery, and PostgreSQL/MySQL upgrade paths.
- Pin release container bases and consume `juno-sdk-go` v1.4.0 deposit-origin contracts.

## v1.4.5 (2026-03-28)

- Fix scanner shutdown ordering so Pebble closes only after scanner and publisher goroutines exit.
- Add RocksDB-backed integration coverage for graceful shutdown during active scanning.

## v1.4.1 (2026-02-10)

- Add multi-daemon docker integration coverage for mempool spend expiry across disconnect/reconnect + sync.
- Add a reusable `junocashd` docker node harness for multi-daemon integration tests.
- Increase docker integration/e2e test timeouts to reduce flakiness on slower hosts.

## v1.4.0 (2026-02-10)

- Persist tx expiry height for mempool spends and use it to keep pending spends sticky until mined or expired.
- Add `OutgoingOutputExpired` event kind with `status.state="expired"` and `payload.expiry_height`.
- Expose `pending_spent_expiry_height` on the notes API.
- Add Postgres/MySQL migrations for:
  - `notes.pending_spent_expiry_height`
  - `outgoing_outputs.tx_expiry_height`
  - `outgoing_outputs.expired_at`
- Add docker integration + e2e coverage for deterministic expiry behavior.
