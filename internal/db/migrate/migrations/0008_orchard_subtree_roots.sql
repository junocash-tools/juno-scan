CREATE TABLE IF NOT EXISTS orchard_subtree_roots (
  subtree_index BIGINT PRIMARY KEY,
  end_position BIGINT NOT NULL,
  end_height BIGINT NOT NULL,
  end_block_hash TEXT NOT NULL,
  root TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS orchard_subtree_roots_end_height_idx ON orchard_subtree_roots(end_height);
