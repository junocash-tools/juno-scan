ALTER TABLE notes
  ADD COLUMN IF NOT EXISTS deposit_eligible BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE notes SET deposit_eligible = FALSE, confirmed_height = NULL;

DELETE FROM events
WHERE kind IN ('DepositEvent', 'DepositConfirmed', 'DepositOrphaned', 'DepositUnconfirmed');

UPDATE wallet_backfill_progress
SET next_height = birthday_height,
    state = 'pending',
    last_error = '',
    updated_at = NOW();

CREATE TABLE IF NOT EXISTS scanner_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
