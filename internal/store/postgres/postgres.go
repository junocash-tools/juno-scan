package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Abdullah1738/juno-scan/internal/db/migrate"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string, schema string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("postgres: dsn is required")
	}
	if strings.TrimSpace(schema) == "" {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("postgres: connect: %w", err)
		}
		return &Store{pool: pool}, nil
	}

	adminConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if _, err := adminConn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
		_ = adminConn.Close(ctx)
		return nil, fmt.Errorf("postgres: create schema: %w", err)
	}
	_ = adminConn.Close(ctx)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse: %w", err)
	}
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *Store) Migrate(ctx context.Context) error {
	return migrate.Apply(ctx, s.pool)
}

func (s *Store) WithTx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(&pgTx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

func (s *Store) UpsertWallet(ctx context.Context, walletID, ufvk string) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO wallets (wallet_id, ufvk, disabled_at)
VALUES ($1, $2, NULL)
ON CONFLICT (wallet_id)
DO UPDATE SET ufvk = EXCLUDED.ufvk, disabled_at = NULL
`, walletID, ufvk)
	if err != nil {
		return fmt.Errorf("postgres: upsert wallet: %w", err)
	}
	return nil
}

func (s *Store) ListWallets(ctx context.Context) ([]store.Wallet, error) {
	rows, err := s.pool.Query(ctx, `SELECT wallet_id, created_at, disabled_at FROM wallets ORDER BY wallet_id`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list wallets: %w", err)
	}
	defer rows.Close()

	var out []store.Wallet
	for rows.Next() {
		var w store.Wallet
		if err := rows.Scan(&w.WalletID, &w.CreatedAt, &w.DisabledAt); err != nil {
			return nil, fmt.Errorf("postgres: list wallets: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list wallets: %w", err)
	}
	return out, nil
}

func (s *Store) ListEnabledWalletUFVKs(ctx context.Context) ([]store.WalletUFVK, error) {
	rows, err := s.pool.Query(ctx, `SELECT wallet_id, ufvk FROM wallets WHERE disabled_at IS NULL ORDER BY wallet_id`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list enabled wallets: %w", err)
	}
	defer rows.Close()

	var out []store.WalletUFVK
	for rows.Next() {
		var w store.WalletUFVK
		if err := rows.Scan(&w.WalletID, &w.UFVK); err != nil {
			return nil, fmt.Errorf("postgres: list enabled wallets: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list enabled wallets: %w", err)
	}
	return out, nil
}

func (s *Store) Tip(ctx context.Context) (store.BlockTip, bool, error) {
	var tip store.BlockTip
	if err := s.pool.QueryRow(ctx, `SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1`).Scan(&tip.Height, &tip.Hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.BlockTip{}, false, nil
		}
		return store.BlockTip{}, false, fmt.Errorf("postgres: tip: %w", err)
	}
	return tip, true, nil
}

func (s *Store) HashAtHeight(ctx context.Context, height int64) (string, bool, error) {
	var hash string
	if err := s.pool.QueryRow(ctx, `SELECT hash FROM blocks WHERE height=$1`, height).Scan(&hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("postgres: hash at height %d: %w", height, err)
	}
	return hash, true, nil
}

func (s *Store) RollbackToHeight(ctx context.Context, height int64) error {
	return s.WithTx(ctx, func(tx store.Tx) error {
		pgtx := tx.(*pgTx)

		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM events WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback events: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM orchard_actions WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback actions: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM orchard_commitments WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback commitments: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM notes WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback notes: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET spent_height = NULL, spent_txid = NULL, spent_confirmed_height = NULL WHERE spent_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unspend: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET confirmed_height = NULL WHERE confirmed_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unconfirm: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET spent_confirmed_height = NULL WHERE spent_confirmed_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unconfirm spend: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM blocks WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback blocks: %w", err)
		}
		return nil
	})
}

func (s *Store) WalletEventPublishCursor(ctx context.Context, walletID string) (int64, error) {
	var cursor int64
	if err := s.pool.QueryRow(ctx, `SELECT cursor FROM wallet_event_publish_cursors WHERE wallet_id = $1`, walletID).Scan(&cursor); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: get publish cursor: %w", err)
	}
	return cursor, nil
}

func (s *Store) SetWalletEventPublishCursor(ctx context.Context, walletID string, cursor int64) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO wallet_event_publish_cursors (wallet_id, cursor)
VALUES ($1, $2)
ON CONFLICT (wallet_id)
DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()
`, walletID, cursor)
	if err != nil {
		return fmt.Errorf("postgres: set publish cursor: %w", err)
	}
	return nil
}

func (s *Store) ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int) ([]store.Event, int64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
SELECT id, kind, height, payload, created_at
FROM events
WHERE wallet_id = $1 AND id > $2
ORDER BY id
LIMIT $3
`, walletID, afterID, limit)
	if err != nil {
		return nil, afterID, fmt.Errorf("postgres: list events: %w", err)
	}
	defer rows.Close()

	var events []store.Event
	nextCursor := afterID
	for rows.Next() {
		var e store.Event
		if err := rows.Scan(&e.ID, &e.Kind, &e.Height, &e.Payload, &e.CreatedAt); err != nil {
			return nil, afterID, fmt.Errorf("postgres: list events: %w", err)
		}
		e.WalletID = walletID
		nextCursor = e.ID
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("postgres: list events: %w", err)
	}
	return events, nextCursor, nil
}

func (s *Store) ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]store.Note, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	query := `
SELECT txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE wallet_id = $1
`
	if onlyUnspent {
		query += " AND spent_height IS NULL"
	}
	query += " ORDER BY height, txid, action_index LIMIT $2"

	rows, err := s.pool.Query(ctx, query, walletID, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list notes: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var divIdx int64
		var memo sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
		n.WalletID = walletID
		if err := rows.Scan(
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&n.Position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&n.SpentHeight,
			&n.SpentTxID,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: list notes: %w", err)
		}
		n.DiversifierIndex = uint32(divIdx)
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if confirmedHeight.Valid {
			n.ConfirmedHeight = &confirmedHeight.Int64
		}
		if spentConfirmedHeight.Valid {
			n.SpentConfirmedHeight = &spentConfirmedHeight.Int64
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list notes: %w", err)
	}
	return out, nil
}

func (s *Store) ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]store.OrchardCommitment, error) {
	rows, err := s.pool.Query(ctx, `
SELECT position, height, txid, action_index, cmx
FROM orchard_commitments
WHERE height <= $1
ORDER BY position
`, height)
	if err != nil {
		return nil, fmt.Errorf("postgres: list commitments: %w", err)
	}
	defer rows.Close()

	var out []store.OrchardCommitment
	for rows.Next() {
		var c store.OrchardCommitment
		if err := rows.Scan(&c.Position, &c.Height, &c.TxID, &c.ActionIndex, &c.CMX); err != nil {
			return nil, fmt.Errorf("postgres: list commitments: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list commitments: %w", err)
	}
	return out, nil
}

type pgTx struct {
	tx pgx.Tx
}

func (t *pgTx) InsertBlock(ctx context.Context, b store.Block) error {
	_, err := t.tx.Exec(ctx, `
INSERT INTO blocks (height, hash, prev_hash, time)
VALUES ($1, $2, $3, $4)
ON CONFLICT (height) DO NOTHING
`, b.Height, b.Hash, b.PrevHash, b.Time)
	if err != nil {
		return fmt.Errorf("postgres: insert block: %w", err)
	}
	return nil
}

func (t *pgTx) NextOrchardCommitmentPosition(ctx context.Context) (int64, error) {
	var nextPos int64
	if err := t.tx.QueryRow(ctx, `SELECT COALESCE(MAX(position) + 1, 0) FROM orchard_commitments`).Scan(&nextPos); err != nil {
		return 0, fmt.Errorf("postgres: next position: %w", err)
	}
	return nextPos, nil
}

func (t *pgTx) InsertOrchardAction(ctx context.Context, a store.OrchardAction) error {
	_, err := t.tx.Exec(ctx, `
INSERT INTO orchard_actions (height, txid, action_index, action_nullifier, cmx, ephemeral_key, enc_ciphertext)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (txid, action_index) DO NOTHING
`, a.Height, a.TxID, a.ActionIndex, a.ActionNullifier, a.CMX, a.EphemeralKey, a.EncCiphertext)
	if err != nil {
		return fmt.Errorf("postgres: insert action: %w", err)
	}
	return nil
}

func (t *pgTx) InsertOrchardCommitment(ctx context.Context, c store.OrchardCommitment) error {
	_, err := t.tx.Exec(ctx, `
INSERT INTO orchard_commitments (position, height, txid, action_index, cmx)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (position) DO NOTHING
`, c.Position, c.Height, c.TxID, c.ActionIndex, c.CMX)
	if err != nil {
		return fmt.Errorf("postgres: insert commitment: %w", err)
	}
	return nil
}

func (t *pgTx) MarkNotesSpent(ctx context.Context, height int64, txid string, nullifiers []string) ([]store.Note, error) {
	rows, err := t.tx.Query(ctx, `
UPDATE notes
SET spent_height = $1, spent_txid = $2, spent_confirmed_height = NULL
WHERE spent_height IS NULL AND note_nullifier = ANY($3::text[])
RETURNING wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
`, height, txid, nullifiers)
	if err != nil {
		return nil, fmt.Errorf("postgres: mark spent: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var divIdx int64
		var memo sql.NullString
		var confirmed sql.NullInt64
		var spentConfirmed sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&n.Position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&n.SpentHeight,
			&n.SpentTxID,
			&confirmed,
			&spentConfirmed,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: mark spent: %w", err)
		}
		n.DiversifierIndex = uint32(divIdx)
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if confirmed.Valid {
			n.ConfirmedHeight = &confirmed.Int64
		}
		if spentConfirmed.Valid {
			n.SpentConfirmedHeight = &spentConfirmed.Int64
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: mark spent: %w", err)
	}
	return out, nil
}

func (t *pgTx) InsertNote(ctx context.Context, n store.Note) error {
	_, err := t.tx.Exec(ctx, `
INSERT INTO notes (
  wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (wallet_id, txid, action_index) DO NOTHING
`, n.WalletID, n.TxID, n.ActionIndex, n.Height, n.Position, int64(n.DiversifierIndex), n.RecipientAddress, n.ValueZat, n.MemoHex, n.NoteNullifier)
	if err != nil {
		return fmt.Errorf("postgres: insert note: %w", err)
	}
	return nil
}

func (t *pgTx) ConfirmNotes(ctx context.Context, confirmationHeight int64, maxNoteHeight int64) ([]store.Note, error) {
	rows, err := t.tx.Query(ctx, `
UPDATE notes
SET confirmed_height = $1
WHERE confirmed_height IS NULL AND height <= $2
RETURNING wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
`, confirmationHeight, maxNoteHeight)
	if err != nil {
		return nil, fmt.Errorf("postgres: confirm notes: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var divIdx int64
		var memo sql.NullString
		var confirmed sql.NullInt64
		var spentConfirmed sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&n.Position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&n.SpentHeight,
			&n.SpentTxID,
			&confirmed,
			&spentConfirmed,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: confirm notes: %w", err)
		}
		n.DiversifierIndex = uint32(divIdx)
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if confirmed.Valid {
			n.ConfirmedHeight = &confirmed.Int64
		}
		if spentConfirmed.Valid {
			n.SpentConfirmedHeight = &spentConfirmed.Int64
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: confirm notes: %w", err)
	}
	return out, nil
}

func (t *pgTx) ConfirmSpends(ctx context.Context, confirmationHeight int64, maxSpentHeight int64) ([]store.Note, error) {
	rows, err := t.tx.Query(ctx, `
UPDATE notes
SET spent_confirmed_height = $1
WHERE spent_height IS NOT NULL AND spent_confirmed_height IS NULL AND spent_height <= $2
RETURNING wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
`, confirmationHeight, maxSpentHeight)
	if err != nil {
		return nil, fmt.Errorf("postgres: confirm spends: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var divIdx int64
		var memo sql.NullString
		var confirmed sql.NullInt64
		var spentConfirmed sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&n.Position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&n.SpentHeight,
			&n.SpentTxID,
			&confirmed,
			&spentConfirmed,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: confirm spends: %w", err)
		}
		n.DiversifierIndex = uint32(divIdx)
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if confirmed.Valid {
			n.ConfirmedHeight = &confirmed.Int64
		}
		if spentConfirmed.Valid {
			n.SpentConfirmedHeight = &spentConfirmed.Int64
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: confirm spends: %w", err)
	}
	return out, nil
}

func (t *pgTx) InsertEvent(ctx context.Context, e store.Event) error {
	_, err := t.tx.Exec(ctx, `
INSERT INTO events (kind, wallet_id, height, payload)
VALUES ($1, $2, $3, $4::jsonb)
`, e.Kind, e.WalletID, e.Height, string(e.Payload))
	if err != nil {
		return fmt.Errorf("postgres: insert event: %w", err)
	}
	return nil
}
