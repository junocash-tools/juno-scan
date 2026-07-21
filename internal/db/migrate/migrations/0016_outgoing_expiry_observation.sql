ALTER TABLE outgoing_outputs
  ADD COLUMN IF NOT EXISTS expired_height BIGINT;

UPDATE outgoing_outputs AS o
SET expired_height = (
  SELECT MAX(e.height)
  FROM events AS e
  WHERE e.kind = 'OutgoingOutputExpired'
    AND e.wallet_id = o.wallet_id
    AND e.payload->>'txid' = o.txid
    AND e.payload->>'action_index' = o.action_index::text
)
WHERE o.expired_at IS NOT NULL
  AND o.expired_height IS NULL;

-- A normal legacy row always has a matching event from the same transaction.
-- The sentinel makes a damaged event journal fail safe: the next rollback
-- unexpires the row so the event can be emitted again.
UPDATE outgoing_outputs
SET expired_height = 9223372036854775807
WHERE expired_at IS NOT NULL
  AND expired_height IS NULL;

CREATE INDEX IF NOT EXISTS outgoing_outputs_expired_height_idx ON outgoing_outputs(expired_height);
