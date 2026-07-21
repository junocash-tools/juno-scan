CREATE INDEX IF NOT EXISTS notes_wallet_recipient_height_idx
ON notes(wallet_id, recipient_address, height);
