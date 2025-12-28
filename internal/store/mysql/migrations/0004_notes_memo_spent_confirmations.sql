ALTER TABLE notes
  ADD COLUMN memo_hex TEXT NULL;

ALTER TABLE notes
  ADD COLUMN spent_confirmed_height BIGINT NULL;

CREATE INDEX notes_spent_confirmed_height_idx ON notes(spent_confirmed_height, spent_height);

