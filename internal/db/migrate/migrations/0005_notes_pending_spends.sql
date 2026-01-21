ALTER TABLE notes
  ADD COLUMN pending_spent_txid TEXT,
  ADD COLUMN pending_spent_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS notes_nullifier_idx ON notes(note_nullifier);
CREATE INDEX IF NOT EXISTS notes_pending_spent_txid_idx ON notes(pending_spent_txid) WHERE pending_spent_txid IS NOT NULL;
