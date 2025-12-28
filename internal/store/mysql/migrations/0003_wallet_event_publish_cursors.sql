CREATE TABLE wallet_event_publish_cursors (
  wallet_id VARCHAR(255) PRIMARY KEY,
  `cursor` BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT wallet_event_publish_cursors_wallet_fk FOREIGN KEY (wallet_id) REFERENCES wallets(wallet_id)
);
