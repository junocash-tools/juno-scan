ALTER TABLE wallets ADD COLUMN birthday_height BIGINT NOT NULL DEFAULT 0;

CREATE TABLE wallet_backfill_progress (
  wallet_id VARCHAR(255) PRIMARY KEY,
  birthday_height BIGINT NOT NULL,
  next_height BIGINT NOT NULL,
  target_height BIGINT NOT NULL DEFAULT 0,
  state VARCHAR(32) NOT NULL DEFAULT 'pending',
  last_error TEXT NOT NULL,
  generation BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT wallet_backfill_wallet_fk FOREIGN KEY (wallet_id) REFERENCES wallets(wallet_id) ON DELETE CASCADE
);

INSERT IGNORE INTO wallet_backfill_progress (wallet_id,birthday_height,next_height,target_height,state,last_error,generation)
SELECT wallet_id,birthday_height,birthday_height,0,'pending','',1 FROM wallets;
