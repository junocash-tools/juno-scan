CREATE TABLE orchard_subtree_roots (
  subtree_index BIGINT PRIMARY KEY,
  end_position BIGINT NOT NULL,
  end_height BIGINT NOT NULL,
  end_block_hash VARCHAR(64) NOT NULL,
  root VARCHAR(64) NOT NULL
);

CREATE INDEX orchard_subtree_roots_end_height_idx ON orchard_subtree_roots(end_height);
