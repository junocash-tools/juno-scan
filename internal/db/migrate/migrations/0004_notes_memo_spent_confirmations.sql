ALTER TABLE notes
  ADD COLUMN IF NOT EXISTS memo_hex TEXT;

ALTER TABLE notes
  ADD COLUMN IF NOT EXISTS spent_confirmed_height BIGINT;

CREATE INDEX IF NOT EXISTS notes_unconfirmed_spends_height_idx
  ON notes(spent_height)
  WHERE spent_height IS NOT NULL AND spent_confirmed_height IS NULL;

