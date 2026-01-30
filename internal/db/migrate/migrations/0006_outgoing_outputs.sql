CREATE TABLE outgoing_outputs (
  wallet_id TEXT NOT NULL REFERENCES wallets(wallet_id),
  txid TEXT NOT NULL,
  action_index INT NOT NULL,
  mined_height BIGINT,
  confirmed_height BIGINT,
  mempool_seen_at TIMESTAMPTZ,
  recipient_address TEXT NOT NULL,
  value_zat BIGINT NOT NULL,
  memo_hex TEXT,
  ovk_scope TEXT NOT NULL,
  recipient_scope TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (wallet_id, txid, action_index)
);

CREATE INDEX outgoing_outputs_mined_height_idx ON outgoing_outputs(mined_height);
CREATE INDEX outgoing_outputs_confirmed_height_idx ON outgoing_outputs(confirmed_height);
