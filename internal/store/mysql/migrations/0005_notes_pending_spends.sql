ALTER TABLE notes
  ADD COLUMN pending_spent_txid VARCHAR(64) NULL,
  ADD COLUMN pending_spent_at TIMESTAMP NULL;

CREATE INDEX notes_nullifier_idx ON notes(note_nullifier);
CREATE INDEX notes_pending_spent_txid_idx ON notes(pending_spent_txid);
