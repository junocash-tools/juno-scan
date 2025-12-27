CREATE TABLE wallets (
  wallet_id TEXT PRIMARY KEY,
  ufvk TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ
);

CREATE TABLE blocks (
  height BIGINT PRIMARY KEY,
  hash TEXT NOT NULL,
  prev_hash TEXT,
  time BIGINT,
  scanned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orchard_actions (
  height BIGINT NOT NULL,
  txid TEXT NOT NULL,
  action_index INT NOT NULL,
  action_nullifier TEXT NOT NULL,
  cmx TEXT NOT NULL,
  ephemeral_key TEXT NOT NULL,
  enc_ciphertext TEXT NOT NULL,
  PRIMARY KEY (txid, action_index)
);

CREATE INDEX orchard_actions_height_idx ON orchard_actions(height);

CREATE TABLE orchard_commitments (
  position BIGINT PRIMARY KEY,
  height BIGINT NOT NULL,
  txid TEXT NOT NULL,
  action_index INT NOT NULL,
  cmx TEXT NOT NULL
);

CREATE INDEX orchard_commitments_height_idx ON orchard_commitments(height);

CREATE TABLE notes (
  wallet_id TEXT NOT NULL REFERENCES wallets(wallet_id),
  txid TEXT NOT NULL,
  action_index INT NOT NULL,
  height BIGINT NOT NULL,
  position BIGINT,
  recipient_address TEXT NOT NULL,
  value_zat BIGINT NOT NULL,
  note_nullifier TEXT NOT NULL,
  spent_height BIGINT,
  spent_txid TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (wallet_id, txid, action_index)
);

CREATE INDEX notes_wallet_height_idx ON notes(wallet_id, height);
CREATE INDEX notes_wallet_spent_height_idx ON notes(wallet_id, spent_height);
CREATE UNIQUE INDEX notes_wallet_nullifier_idx ON notes(wallet_id, note_nullifier);

CREATE TABLE events (
  id BIGSERIAL PRIMARY KEY,
  kind TEXT NOT NULL,
  wallet_id TEXT NOT NULL,
  height BIGINT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX events_wallet_id_idx ON events(wallet_id, id);
CREATE INDEX events_height_idx ON events(height);

