# Wallet backfill (v1)

When a wallet (UFVK) is added after the scanner has already indexed past blocks, it will only be detected going forward until you backfill it.

Backfill is performed via:

- `POST /v1/wallets/{wallet_id}/backfill`

## Request

```json
{ "from_height": 0, "to_height": 12345, "batch_size": 1000 }
```

Notes:

- `from_height` defaults to `0`
- `to_height` defaults to the current scanned tip height
- `batch_size` limits the number of blocks processed in a single call (default `100`, max `10000`)

## Response

The response includes `next_height`; repeat calls starting from `next_height` until it exceeds `to_height`.

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

Backfill is idempotent: re-running the same range will not duplicate `DepositEvent` rows for already-known notes.

