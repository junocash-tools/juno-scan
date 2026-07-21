# Wallet backfill (v1)

When a wallet (UFVK) is added after the scanner has already indexed past blocks, it will only be detected going forward until you backfill it.

Backfill is performed via:

- `POST /v1/wallets/{wallet_id}/backfill`

## Request

```json
{ "from_height": 0, "to_height": 12345, "batch_size": 1000 }
```

Notes:

- `from_height` defaults to the persisted `next_height`; if supplied, it must match exactly
- `to_height` defaults to the current scanned tip height
- `batch_size` limits the number of blocks processed in a single call (default `100`, max `10000`)

The scanner's Orchard action-height index skips verbose block RPCs for empty heights. A barren batch uses one final canonical fence, and each non-empty height commits separately so foreground reads can proceed between blocks.

## Response

The response includes `next_height`; repeat calls without `from_height`, or pass that exact value. Progress is complete only when `next_height` is beyond the current scanned tip.

```json
{
  "status": "ok",
  "wallet_id": "hot",
  "from_height": 0,
  "to_height": 12345,
  "scanned_from": 0,
  "scanned_to": 999,
  "next_height": 1000,
  "inserted_notes": 1,
  "inserted_events": 2
}
```

Repeating the default request after completion is a successful no-op. It performs no RPC calls and does not change progress or readiness. Explicit ranges where `to_height` is below the persisted `next_height` are rejected without changing progress.

Each processed height is fenced against both the node's canonical block hash and the scanner's locked block row. A reorg race aborts without advancing progress. Progress uses a generation and next-height compare-and-set, so an in-flight request cannot overwrite a birthday rewind or a newer batch.

Backfill mutations are idempotent, but completed ranges cannot be replayed through the API. To scan earlier history, register an earlier birthday; that resets progress and advances its generation.
