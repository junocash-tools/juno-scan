CREATE TABLE orchard_shard_roots (
  shard_index BIGINT PRIMARY KEY,
  version INT NOT NULL,
  end_position BIGINT NOT NULL,
  end_height BIGINT NOT NULL,
  end_block_hash VARCHAR(64) NOT NULL,
  root VARCHAR(64) NOT NULL,
  INDEX orchard_shard_roots_end_height_idx (end_height)
);

CREATE TABLE orchard_shard_cache_state (
  singleton TINYINT PRIMARY KEY,
  version INT NOT NULL,
  next_index BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT IGNORE INTO orchard_shard_cache_state (singleton, version, next_index)
VALUES (1, 1, 0);
