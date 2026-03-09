ALTER TABLE outgoing_outputs
  ADD COLUMN IF NOT EXISTS position BIGINT;

CREATE INDEX IF NOT EXISTS outgoing_outputs_wallet_mined_height_idx
  ON outgoing_outputs(wallet_id, mined_height, txid, action_index)
  WHERE mined_height IS NOT NULL;
