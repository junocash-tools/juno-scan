CREATE TABLE outgoing_outputs (
  wallet_id VARCHAR(255) NOT NULL,
  txid VARCHAR(64) NOT NULL,
  action_index INT NOT NULL,
  mined_height BIGINT,
  confirmed_height BIGINT,
  mempool_seen_at DATETIME(6),
  recipient_address TEXT NOT NULL,
  value_zat BIGINT NOT NULL,
  memo_hex TEXT,
  ovk_scope TEXT NOT NULL,
  recipient_scope TEXT,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (wallet_id, txid, action_index),
  CONSTRAINT outgoing_outputs_wallet_fk FOREIGN KEY (wallet_id) REFERENCES wallets(wallet_id)
);

CREATE INDEX outgoing_outputs_mined_height_idx ON outgoing_outputs(mined_height);
CREATE INDEX outgoing_outputs_confirmed_height_idx ON outgoing_outputs(confirmed_height);
