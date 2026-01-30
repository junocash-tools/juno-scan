# juno-scan event payloads (v1)

Wallet events are returned by:

- `GET /v1/wallets/{wallet_id}/events`

Query parameters:

- `cursor` / `limit`: paginate by event id.
- `block_height` (optional): filter to events emitted at a specific height (debug/audit only; not recommended due to reorg risk).
- `kind` (optional): filter to one or more event kinds (repeat `kind=` or pass a comma-separated list).
- `txid` (optional): filter to events where `payload.txid` matches the given txid (lowercase hex).

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

## Outgoing output events

Outgoing output events expose the *outputs created by a wallet's spends* (one per Orchard action) so callers can reconcile what was sent (and what change was created).

Limitations:

- Outgoing output details (`recipient_address`, `amount_zatoshis`, `memo_hex`) are recovered via **OVK output recovery** from the wallet's UFVK; they are not visible from the chain without wallet keys.
- For `OutgoingOutputEvent` observed in the mempool, `payload.height` is omitted and the top-level event `height` is `0` (not meaningful). Use `payload.status.state == "mempool"`.

### `OutgoingOutputEvent`

Fields:

- `version`, `wallet_id`
- `txid`
- `height` (optional): present when mined
- `action_index`
- `amount_zatoshis`
- `recipient_address`: unified address (j1...)
- `memo_hex` (optional)
- `ovk_scope`: `"external"` or `"internal"` (which OVK recovered the output)
- `recipient_scope` (optional): `"external"` or `"internal"` when the recovered recipient belongs to this wallet (useful for change detection)
- `status`

### `OutgoingOutputConfirmed`

`OutgoingOutputEvent` fields plus:

- `confirmed_height`
- `required_confirmations`

### `OutgoingOutputOrphaned`

`OutgoingOutputEvent` fields plus:

- `orphaned_at_height`

### `OutgoingOutputUnconfirmed`

`OutgoingOutputEvent` fields plus:

- `rollback_height`
- `required_confirmations` (optional)
- `previous_confirmed_height`
