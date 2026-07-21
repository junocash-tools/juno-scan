CREATE TABLE orchard_shard_roots (
  shard_index BIGINT PRIMARY KEY,
  version INTEGER NOT NULL,
  end_position BIGINT NOT NULL,
  end_height BIGINT NOT NULL,
  end_block_hash VARCHAR(64) NOT NULL,
  root VARCHAR(64) NOT NULL
);

CREATE INDEX orchard_shard_roots_end_height_idx ON orchard_shard_roots(end_height);

CREATE TABLE orchard_shard_cache_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  version INTEGER NOT NULL,
  next_index BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO orchard_shard_cache_state (singleton, version, next_index)
VALUES (TRUE, 1, 0)
ON CONFLICT (singleton) DO NOTHING;
