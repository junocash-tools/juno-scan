# Changelog

## v1.4.0 (2026-02-10)

- Persist tx expiry height for mempool spends and use it to keep pending spends sticky until mined or expired.
- Add `OutgoingOutputExpired` event kind with `status.state="expired"` and `payload.expiry_height`.
- Expose `pending_spent_expiry_height` on the notes API.
- Add Postgres/MySQL migrations for:
  - `notes.pending_spent_expiry_height`
  - `outgoing_outputs.tx_expiry_height`
  - `outgoing_outputs.expired_at`
- Add docker integration + e2e coverage for deterministic expiry behavior.

