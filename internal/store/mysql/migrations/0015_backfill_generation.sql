ALTER TABLE wallet_backfill_progress
  ADD COLUMN generation BIGINT NOT NULL DEFAULT 1;

UPDATE wallet_backfill_progress SET generation=1 WHERE generation < 1;

INSERT IGNORE INTO wallet_backfill_progress (wallet_id,birthday_height,next_height,target_height,state,last_error,generation)
SELECT wallet_id,birthday_height,birthday_height,0,'pending','',1 FROM wallets;
