CREATE TABLE wallets (
  wallet_id VARCHAR(255) PRIMARY KEY,
  ufvk TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  disabled_at TIMESTAMP NULL
);

CREATE TABLE blocks (
  height BIGINT PRIMARY KEY,
  hash TEXT NOT NULL,
  prev_hash TEXT,
  time BIGINT,
  scanned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orchard_actions (
  height BIGINT NOT NULL,
  txid VARCHAR(64) NOT NULL,
  action_index INT NOT NULL,
  action_nullifier VARCHAR(64) NOT NULL,
  cmx VARCHAR(64) NOT NULL,
  ephemeral_key VARCHAR(64) NOT NULL,
  enc_ciphertext TEXT NOT NULL,
  PRIMARY KEY (txid, action_index)
);

CREATE INDEX orchard_actions_height_idx ON orchard_actions(height);

CREATE TABLE orchard_commitments (
  position BIGINT PRIMARY KEY,
  height BIGINT NOT NULL,
  txid VARCHAR(64) NOT NULL,
  action_index INT NOT NULL,
  cmx VARCHAR(64) NOT NULL
);

CREATE INDEX orchard_commitments_height_idx ON orchard_commitments(height);

CREATE TABLE notes (
  wallet_id VARCHAR(255) NOT NULL,
  txid VARCHAR(64) NOT NULL,
  action_index INT NOT NULL,
  height BIGINT NOT NULL,
  position BIGINT,
  recipient_address TEXT NOT NULL,
  value_zat BIGINT NOT NULL,
  note_nullifier VARCHAR(64) NOT NULL,
  spent_height BIGINT,
  spent_txid VARCHAR(64),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (wallet_id, txid, action_index),
  CONSTRAINT notes_wallet_fk FOREIGN KEY (wallet_id) REFERENCES wallets(wallet_id)
);

CREATE INDEX notes_wallet_height_idx ON notes(wallet_id, height);
CREATE INDEX notes_wallet_spent_height_idx ON notes(wallet_id, spent_height);
CREATE UNIQUE INDEX notes_wallet_nullifier_idx ON notes(wallet_id, note_nullifier);

CREATE TABLE events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  kind VARCHAR(64) NOT NULL,
  wallet_id VARCHAR(255) NOT NULL,
  height BIGINT NOT NULL,
  payload JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX events_wallet_id_idx ON events(wallet_id, id);
CREATE INDEX events_height_idx ON events(height);

