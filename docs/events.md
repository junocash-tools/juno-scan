# juno-scan event payloads (v1)

Wallet events are returned by:

- `GET /v1/wallets/{wallet_id}/events`

Query parameters:

- `cursor` / `limit`: paginate by event id.
- `block_height` (optional): filter to events emitted at a specific height (debug/audit only; not recommended due to reorg risk).

Each item has:

- `kind`: identifies the payload type
- `payload`: the event payload JSON object

This document describes the `payload` shapes for each `kind`.

## Common types

### `version`

All event payloads include `version` (string). The current event payload version is `"v1"`.

### `status`

Event payloads include `status` describing the observed tx lifecycle:

```json
{ "state": "mempool|confirmed|orphaned", "height": 123, "confirmations": 5 }
```

Fields:

- `state`: `"mempool"`, `"confirmed"`, or `"orphaned"`
- `height`: present when applicable
- `confirmations`: present when applicable

## Deposit events

### `DepositEvent`

Base fields:

- `version`, `wallet_id`, `account_id` (optional), `diversifier_index` (optional)
- `txid`, `height`, `action_index`
- `amount_zatoshis`, `memo_hex` (optional)
- `status`

Scanner-added fields:

- `recipient_address` (optional): unified address (j1...) if the UFVK allowed recovery
- `note_nullifier` (optional): note nullifier (hex) if available

### `DepositConfirmed`

`DepositEvent` fields plus:

- `confirmed_height`
- `required_confirmations`

### `DepositOrphaned`

`DepositEvent` fields plus:

- `orphaned_at_height`

### `DepositUnconfirmed`

`DepositEvent` fields plus:

- `rollback_height`
- `required_confirmations` (optional)
- `previous_confirmed_height`

## Spend events

### `SpendEvent`

Fields:

- `version`, `wallet_id`, `diversifier_index` (optional)
- `txid`, `height`
- note linkage: `note_txid`, `note_action_index`, `note_height`
- note details: `amount_zatoshis`, `note_nullifier` (optional)
- optional: `recipient_address`
- `status`

### `SpendConfirmed`

`SpendEvent` fields plus:

- `confirmed_height`
- `required_confirmations`

### `SpendOrphaned`

`SpendEvent` fields plus:

- `orphaned_at_height`

### `SpendUnconfirmed`

`SpendEvent` fields plus:

- `rollback_height`
- `required_confirmations` (optional)
- `previous_confirmed_height`
