ALTER TABLE wallets ADD COLUMN IF NOT EXISTS birthday_height BIGINT NOT NULL DEFAULT 0;

CREATE TABLE wallet_backfill_progress (
  wallet_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(wallet_id) ON DELETE CASCADE,
  birthday_height BIGINT NOT NULL,
  next_height BIGINT NOT NULL,
  target_height BIGINT NOT NULL DEFAULT 0,
  state VARCHAR(32) NOT NULL DEFAULT 'pending',
  last_error TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
