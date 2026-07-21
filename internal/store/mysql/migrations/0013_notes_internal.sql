ALTER TABLE notes
  ADD COLUMN deposit_eligible BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE notes SET deposit_eligible = FALSE, confirmed_height = NULL;

DELETE FROM events
WHERE kind IN ('DepositEvent', 'DepositConfirmed', 'DepositOrphaned', 'DepositUnconfirmed');

UPDATE wallet_backfill_progress
SET next_height = birthday_height,
    state = 'pending',
    last_error = '';

CREATE TABLE IF NOT EXISTS scanner_metadata (
  `key` VARCHAR(64) PRIMARY KEY,
  `value` VARCHAR(255) NOT NULL
);
