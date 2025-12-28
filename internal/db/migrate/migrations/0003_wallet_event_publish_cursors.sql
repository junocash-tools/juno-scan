CREATE TABLE IF NOT EXISTS wallet_event_publish_cursors (
  wallet_id TEXT PRIMARY KEY REFERENCES wallets(wallet_id),
  cursor BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
