ALTER TABLE notes
  ADD COLUMN diversifier_index BIGINT NOT NULL DEFAULT 0;

ALTER TABLE notes
  ADD COLUMN confirmed_height BIGINT NULL;

CREATE INDEX notes_confirmed_height_idx ON notes(confirmed_height, height);
