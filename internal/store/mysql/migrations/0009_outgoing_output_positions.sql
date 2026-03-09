ALTER TABLE outgoing_outputs
  ADD COLUMN position BIGINT NULL;

CREATE INDEX outgoing_outputs_wallet_mined_height_idx
  ON outgoing_outputs(wallet_id, mined_height, txid, action_index);
