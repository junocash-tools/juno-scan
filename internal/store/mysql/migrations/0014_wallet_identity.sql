ALTER TABLE wallets
  ADD COLUMN ufvk_sha256 BINARY(32)
  GENERATED ALWAYS AS (UNHEX(SHA2(ufvk, 256))) STORED;

CREATE UNIQUE INDEX wallets_ufvk_unique_idx ON wallets(ufvk_sha256);
