ALTER TABLE notes
  ADD COLUMN IF NOT EXISTS diversifier_index BIGINT NOT NULL DEFAULT 0;

ALTER TABLE notes
  ADD COLUMN IF NOT EXISTS confirmed_height BIGINT;

CREATE INDEX IF NOT EXISTS notes_confirmed_height_idx ON notes(confirmed_height, height);
