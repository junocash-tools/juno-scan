ALTER TABLE notes
  ADD COLUMN pending_spent_expiry_height BIGINT NULL;

ALTER TABLE outgoing_outputs
  ADD COLUMN tx_expiry_height BIGINT NULL,
  ADD COLUMN expired_at DATETIME(6) NULL;

CREATE INDEX outgoing_outputs_tx_expiry_height_idx ON outgoing_outputs(tx_expiry_height);
CREATE INDEX outgoing_outputs_expired_at_idx ON outgoing_outputs(expired_at);
