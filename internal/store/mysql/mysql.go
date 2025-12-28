//go:build mysql

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-sdk-go/types"
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("mysql: dsn is required")
	}

	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: parse dsn: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	return applyMigrations(ctx, s.db)
}

func (s *Store) WithTx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := fn(&myTx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: commit: %w", err)
	}
	return nil
}

func (s *Store) UpsertWallet(ctx context.Context, walletID, ufvk string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO wallets (wallet_id, ufvk, disabled_at)
VALUES (?, ?, NULL)
ON DUPLICATE KEY UPDATE ufvk = VALUES(ufvk), disabled_at = NULL
`, walletID, ufvk)
	if err != nil {
		return fmt.Errorf("mysql: upsert wallet: %w", err)
	}
	return nil
}

func (s *Store) ListWallets(ctx context.Context) ([]store.Wallet, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT wallet_id, created_at, disabled_at FROM wallets ORDER BY wallet_id`)
	if err != nil {
		return nil, fmt.Errorf("mysql: list wallets: %w", err)
	}
	defer rows.Close()

	var out []store.Wallet
	for rows.Next() {
		var w store.Wallet
		var disabled sql.NullTime
		if err := rows.Scan(&w.WalletID, &w.CreatedAt, &disabled); err != nil {
			return nil, fmt.Errorf("mysql: list wallets: %w", err)
		}
		if disabled.Valid {
			w.DisabledAt = &disabled.Time
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list wallets: %w", err)
	}
	return out, nil
}

func (s *Store) ListEnabledWalletUFVKs(ctx context.Context) ([]store.WalletUFVK, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT wallet_id, ufvk FROM wallets WHERE disabled_at IS NULL ORDER BY wallet_id`)
	if err != nil {
		return nil, fmt.Errorf("mysql: list enabled wallets: %w", err)
	}
	defer rows.Close()

	var out []store.WalletUFVK
	for rows.Next() {
		var w store.WalletUFVK
		if err := rows.Scan(&w.WalletID, &w.UFVK); err != nil {
			return nil, fmt.Errorf("mysql: list enabled wallets: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list enabled wallets: %w", err)
	}
	return out, nil
}

func (s *Store) Tip(ctx context.Context) (store.BlockTip, bool, error) {
	var tip store.BlockTip
	if err := s.db.QueryRowContext(ctx, `SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1`).Scan(&tip.Height, &tip.Hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.BlockTip{}, false, nil
		}
		return store.BlockTip{}, false, fmt.Errorf("mysql: tip: %w", err)
	}
	return tip, true, nil
}

func (s *Store) HashAtHeight(ctx context.Context, height int64) (string, bool, error) {
	var hash string
	if err := s.db.QueryRowContext(ctx, `SELECT hash FROM blocks WHERE height=?`, height).Scan(&hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("mysql: hash at height %d: %w", height, err)
	}
	return hash, true, nil
}

func (s *Store) RollbackToHeight(ctx context.Context, height int64) error {
	return s.WithTx(ctx, func(tx store.Tx) error {
		mytx := tx.(*myTx)

		var orphanDeposits []store.Note
		var unconfirmedDeposits []store.Note
		var orphanSpends []store.Note
		var unconfirmedSpends []store.Note

		if height >= 0 {
			fetchNotes := func(query string, args ...any) ([]store.Note, error) {
				rows, err := mytx.tx.QueryContext(ctx, query, args...)
				if err != nil {
					return nil, err
				}
				defer rows.Close()

				var out []store.Note
				for rows.Next() {
					var n store.Note
					var position sql.NullInt64
					var divIdx sql.NullInt64
					var memo sql.NullString
					var spentHeight sql.NullInt64
					var spentTxid sql.NullString
					var confirmedHeight sql.NullInt64
					var spentConfirmedHeight sql.NullInt64
					if err := rows.Scan(
						&n.WalletID,
						&n.TxID,
						&n.ActionIndex,
						&n.Height,
						&position,
						&divIdx,
						&n.RecipientAddress,
						&n.ValueZat,
						&memo,
						&n.NoteNullifier,
						&spentHeight,
						&spentTxid,
						&confirmedHeight,
						&spentConfirmedHeight,
						&n.CreatedAt,
					); err != nil {
						return nil, err
					}
					if position.Valid {
						n.Position = &position.Int64
					}
					if divIdx.Valid {
						n.DiversifierIndex = uint32(divIdx.Int64)
					}
					if memo.Valid {
						n.MemoHex = &memo.String
					}
					if spentHeight.Valid {
						n.SpentHeight = &spentHeight.Int64
					}
					if spentTxid.Valid {
						n.SpentTxID = &spentTxid.String
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
					return nil, err
				}
				return out, nil
			}

			baseSelect := `
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
`

			var err error
			orphanDeposits, err = fetchNotes(baseSelect+`WHERE height > ?`, height)
			if err != nil {
				return fmt.Errorf("mysql: rollback list orphan deposits: %w", err)
			}
			unconfirmedDeposits, err = fetchNotes(baseSelect+`WHERE height <= ? AND confirmed_height IS NOT NULL AND confirmed_height > ?`, height, height)
			if err != nil {
				return fmt.Errorf("mysql: rollback list unconfirmed deposits: %w", err)
			}
			orphanSpends, err = fetchNotes(baseSelect+`WHERE height <= ? AND spent_height IS NOT NULL AND spent_height > ?`, height, height)
			if err != nil {
				return fmt.Errorf("mysql: rollback list orphan spends: %w", err)
			}
			unconfirmedSpends, err = fetchNotes(baseSelect+`WHERE height <= ? AND spent_height IS NOT NULL AND spent_height <= ? AND spent_confirmed_height IS NOT NULL AND spent_confirmed_height > ?`, height, height, height)
			if err != nil {
				return fmt.Errorf("mysql: rollback list unconfirmed spends: %w", err)
			}
		}

		if _, err := mytx.tx.ExecContext(ctx, `DELETE FROM events WHERE height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback events: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `DELETE FROM orchard_actions WHERE height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback actions: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `DELETE FROM orchard_commitments WHERE height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback commitments: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `DELETE FROM notes WHERE height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback notes: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `UPDATE notes SET spent_height = NULL, spent_txid = NULL, spent_confirmed_height = NULL WHERE spent_height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback unspend: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `UPDATE notes SET confirmed_height = NULL WHERE confirmed_height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback unconfirm: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `UPDATE notes SET spent_confirmed_height = NULL WHERE spent_confirmed_height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback unconfirm spend: %w", err)
		}
		if _, err := mytx.tx.ExecContext(ctx, `DELETE FROM blocks WHERE height > ?`, height); err != nil {
			return fmt.Errorf("mysql: rollback blocks: %w", err)
		}

		if height >= 0 {
			for _, n := range orphanDeposits {
				memoHex := ""
				if n.MemoHex != nil {
					memoHex = *n.MemoHex
				}
				payload := events.DepositOrphanedPayload{
					DepositEventPayload: events.DepositEventPayload{
						DepositEvent: types.DepositEvent{
							Version:          types.V1,
							WalletID:         n.WalletID,
							DiversifierIndex: n.DiversifierIndex,
							TxID:             n.TxID,
							Height:           n.Height,
							ActionIndex:      uint32(n.ActionIndex),
							AmountZatoshis:   uint64(n.ValueZat),
							MemoHex:          memoHex,
							Status: types.TxStatus{
								State:  types.TxStateOrphaned,
								Height: n.Height,
							},
						},
						RecipientAddress: n.RecipientAddress,
						NoteNullifier:    n.NoteNullifier,
					},
					OrphanedAtHeight: height,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("mysql: rollback marshal deposit orphaned: %w", err)
				}
				if err := mytx.InsertEvent(ctx, store.Event{
					Kind:     events.KindDepositOrphaned,
					WalletID: n.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}

			for _, n := range unconfirmedDeposits {
				if n.ConfirmedHeight == nil {
					continue
				}
				memoHex := ""
				if n.MemoHex != nil {
					memoHex = *n.MemoHex
				}
				confirmations := height - n.Height + 1
				if confirmations < 0 {
					confirmations = 0
				}
				payload := events.DepositUnconfirmedPayload{
					DepositEventPayload: events.DepositEventPayload{
						DepositEvent: types.DepositEvent{
							Version:          types.V1,
							WalletID:         n.WalletID,
							DiversifierIndex: n.DiversifierIndex,
							TxID:             n.TxID,
							Height:           n.Height,
							ActionIndex:      uint32(n.ActionIndex),
							AmountZatoshis:   uint64(n.ValueZat),
							MemoHex:          memoHex,
							Status: types.TxStatus{
								State:         types.TxStateConfirmed,
								Height:        n.Height,
								Confirmations: confirmations,
							},
						},
						RecipientAddress: n.RecipientAddress,
						NoteNullifier:    n.NoteNullifier,
					},
					RollbackHeight:          height,
					PreviousConfirmedHeight: *n.ConfirmedHeight,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("mysql: rollback marshal deposit unconfirmed: %w", err)
				}
				if err := mytx.InsertEvent(ctx, store.Event{
					Kind:     events.KindDepositUnconfirmed,
					WalletID: n.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}

			for _, n := range orphanSpends {
				if n.SpentHeight == nil || n.SpentTxID == nil {
					continue
				}
				payload := events.SpendOrphanedPayload{
					SpendEventPayload: events.SpendEventPayload{
						Version:          types.V1,
						WalletID:         n.WalletID,
						DiversifierIndex: n.DiversifierIndex,
						TxID:             *n.SpentTxID,
						Height:           *n.SpentHeight,
						NoteTxID:         n.TxID,
						NoteActionIndex:  uint32(n.ActionIndex),
						NoteHeight:       n.Height,
						AmountZatoshis:   uint64(n.ValueZat),
						NoteNullifier:    n.NoteNullifier,
						RecipientAddress: n.RecipientAddress,
						Status: types.TxStatus{
							State:  types.TxStateOrphaned,
							Height: *n.SpentHeight,
						},
					},
					OrphanedAtHeight: height,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("mysql: rollback marshal spend orphaned: %w", err)
				}
				if err := mytx.InsertEvent(ctx, store.Event{
					Kind:     events.KindSpendOrphaned,
					WalletID: n.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}

			for _, n := range unconfirmedSpends {
				if n.SpentHeight == nil || n.SpentTxID == nil || n.SpentConfirmedHeight == nil {
					continue
				}
				confirmations := height - *n.SpentHeight + 1
				if confirmations < 0 {
					confirmations = 0
				}
				payload := events.SpendUnconfirmedPayload{
					SpendEventPayload: events.SpendEventPayload{
						Version:          types.V1,
						WalletID:         n.WalletID,
						DiversifierIndex: n.DiversifierIndex,
						TxID:             *n.SpentTxID,
						Height:           *n.SpentHeight,
						NoteTxID:         n.TxID,
						NoteActionIndex:  uint32(n.ActionIndex),
						NoteHeight:       n.Height,
						AmountZatoshis:   uint64(n.ValueZat),
						NoteNullifier:    n.NoteNullifier,
						RecipientAddress: n.RecipientAddress,
						Status: types.TxStatus{
							State:         types.TxStateConfirmed,
							Height:        *n.SpentHeight,
							Confirmations: confirmations,
						},
					},
					RollbackHeight:          height,
					PreviousConfirmedHeight: *n.SpentConfirmedHeight,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("mysql: rollback marshal spend unconfirmed: %w", err)
				}
				if err := mytx.InsertEvent(ctx, store.Event{
					Kind:     events.KindSpendUnconfirmed,
					WalletID: n.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) WalletEventPublishCursor(ctx context.Context, walletID string) (int64, error) {
	var cursor int64
	if err := s.db.QueryRowContext(ctx, `SELECT cursor FROM wallet_event_publish_cursors WHERE wallet_id = ?`, walletID).Scan(&cursor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("mysql: get publish cursor: %w", err)
	}
	return cursor, nil
}

func (s *Store) SetWalletEventPublishCursor(ctx context.Context, walletID string, cursor int64) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO wallet_event_publish_cursors (wallet_id, cursor)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE cursor = VALUES(cursor)
`, walletID, cursor)
	if err != nil {
		return fmt.Errorf("mysql: set publish cursor: %w", err)
	}
	return nil
}

func (s *Store) ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int) ([]store.Event, int64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, kind, height, payload, created_at
FROM events
WHERE wallet_id = ? AND id > ?
ORDER BY id
LIMIT ?
`, walletID, afterID, limit)
	if err != nil {
		return nil, afterID, fmt.Errorf("mysql: list events: %w", err)
	}
	defer rows.Close()

	var out []store.Event
	nextCursor := afterID
	for rows.Next() {
		var e store.Event
		var payloadBytes []byte
		if err := rows.Scan(&e.ID, &e.Kind, &e.Height, &payloadBytes, &e.CreatedAt); err != nil {
			return nil, afterID, fmt.Errorf("mysql: list events: %w", err)
		}
		e.WalletID = walletID
		e.Payload = json.RawMessage(payloadBytes)
		nextCursor = e.ID
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, afterID, fmt.Errorf("mysql: list events: %w", err)
	}
	return out, nextCursor, nil
}

func (s *Store) ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]store.Note, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	query := `
SELECT txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE wallet_id = ?
`
	if onlyUnspent {
		query += " AND spent_height IS NULL"
	}
	query += " ORDER BY height, txid, action_index LIMIT ?"

	rows, err := s.db.QueryContext(ctx, query, walletID, limit)
	if err != nil {
		return nil, fmt.Errorf("mysql: list notes: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var position sql.NullInt64
		var divIdx sql.NullInt64
		var memo sql.NullString
		var spentHeight sql.NullInt64
		var spentTxid sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
		n.WalletID = walletID
		if err := rows.Scan(
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&spentHeight,
			&spentTxid,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("mysql: list notes: %w", err)
		}
		if position.Valid {
			n.Position = &position.Int64
		}
		if divIdx.Valid {
			n.DiversifierIndex = uint32(divIdx.Int64)
		}
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if spentHeight.Valid {
			n.SpentHeight = &spentHeight.Int64
		}
		if spentTxid.Valid {
			n.SpentTxID = &spentTxid.String
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
		return nil, fmt.Errorf("mysql: list notes: %w", err)
	}
	return out, nil
}

func (s *Store) ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]store.OrchardCommitment, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT position, height, txid, action_index, cmx
FROM orchard_commitments
WHERE height <= ?
ORDER BY position
`, height)
	if err != nil {
		return nil, fmt.Errorf("mysql: list commitments: %w", err)
	}
	defer rows.Close()

	var out []store.OrchardCommitment
	for rows.Next() {
		var c store.OrchardCommitment
		if err := rows.Scan(&c.Position, &c.Height, &c.TxID, &c.ActionIndex, &c.CMX); err != nil {
			return nil, fmt.Errorf("mysql: list commitments: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list commitments: %w", err)
	}
	return out, nil
}

type myTx struct {
	tx *sql.Tx
}

func (t *myTx) InsertBlock(ctx context.Context, b store.Block) error {
	_, err := t.tx.ExecContext(ctx, `
INSERT IGNORE INTO blocks (height, hash, prev_hash, time)
VALUES (?, ?, ?, ?)
`, b.Height, b.Hash, b.PrevHash, b.Time)
	if err != nil {
		return fmt.Errorf("mysql: insert block: %w", err)
	}
	return nil
}

func (t *myTx) NextOrchardCommitmentPosition(ctx context.Context) (int64, error) {
	var nextPos int64
	if err := t.tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(position) + 1, 0) FROM orchard_commitments`).Scan(&nextPos); err != nil {
		return 0, fmt.Errorf("mysql: next position: %w", err)
	}
	return nextPos, nil
}

func (t *myTx) InsertOrchardAction(ctx context.Context, a store.OrchardAction) error {
	_, err := t.tx.ExecContext(ctx, `
INSERT IGNORE INTO orchard_actions (height, txid, action_index, action_nullifier, cmx, ephemeral_key, enc_ciphertext)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, a.Height, a.TxID, a.ActionIndex, a.ActionNullifier, a.CMX, a.EphemeralKey, a.EncCiphertext)
	if err != nil {
		return fmt.Errorf("mysql: insert action: %w", err)
	}
	return nil
}

func (t *myTx) InsertOrchardCommitment(ctx context.Context, c store.OrchardCommitment) error {
	_, err := t.tx.ExecContext(ctx, `
INSERT IGNORE INTO orchard_commitments (position, height, txid, action_index, cmx)
VALUES (?, ?, ?, ?, ?)
`, c.Position, c.Height, c.TxID, c.ActionIndex, c.CMX)
	if err != nil {
		return fmt.Errorf("mysql: insert commitment: %w", err)
	}
	return nil
}

func (t *myTx) MarkNotesSpent(ctx context.Context, height int64, txid string, nullifiers []string) ([]store.Note, error) {
	if len(nullifiers) == 0 {
		return nil, nil
	}

	args := make([]any, 0, 2+len(nullifiers))
	args = append(args, height, txid)

	var b strings.Builder
	b.WriteString(`
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE spent_height IS NULL AND note_nullifier IN (`)
	for i, nf := range nullifiers {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("?")
		args = append(args, nf)
	}
	b.WriteString(") ORDER BY wallet_id, txid, action_index")

	rows, err := t.tx.QueryContext(ctx, b.String(), args[2:]...)
	if err != nil {
		return nil, fmt.Errorf("mysql: mark spent list: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var position sql.NullInt64
		var divIdx sql.NullInt64
		var memo sql.NullString
		var spentHeight sql.NullInt64
		var spentTxid sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&spentHeight,
			&spentTxid,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("mysql: mark spent list: %w", err)
		}
		if position.Valid {
			n.Position = &position.Int64
		}
		if divIdx.Valid {
			n.DiversifierIndex = uint32(divIdx.Int64)
		}
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if spentHeight.Valid {
			n.SpentHeight = &spentHeight.Int64
		}
		if spentTxid.Valid {
			n.SpentTxID = &spentTxid.String
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
		return nil, fmt.Errorf("mysql: mark spent list: %w", err)
	}

	if len(out) == 0 {
		return nil, nil
	}

	args = args[:2]

	b.Reset()
	b.WriteString(`
UPDATE notes
SET spent_height = ?, spent_txid = ?, spent_confirmed_height = NULL
WHERE spent_height IS NULL AND note_nullifier IN (`)
	for i, nf := range nullifiers {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("?")
		args = append(args, nf)
	}
	b.WriteString(")")

	if _, err := t.tx.ExecContext(ctx, b.String(), args...); err != nil {
		return nil, fmt.Errorf("mysql: mark spent update: %w", err)
	}

	for i := range out {
		out[i].SpentHeight = &height
		out[i].SpentTxID = &txid
		out[i].SpentConfirmedHeight = nil
	}
	return out, nil
}

func (t *myTx) InsertNote(ctx context.Context, n store.Note) error {
	_, err := t.tx.ExecContext(ctx, `
INSERT IGNORE INTO notes (
  wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, n.WalletID, n.TxID, n.ActionIndex, n.Height, n.Position, int64(n.DiversifierIndex), n.RecipientAddress, n.ValueZat, n.MemoHex, n.NoteNullifier)
	if err != nil {
		return fmt.Errorf("mysql: insert note: %w", err)
	}
	return nil
}

func (t *myTx) ConfirmNotes(ctx context.Context, confirmationHeight int64, maxNoteHeight int64) ([]store.Note, error) {
	rows, err := t.tx.QueryContext(ctx, `
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE confirmed_height IS NULL AND height <= ?
ORDER BY height, wallet_id, txid, action_index
`, maxNoteHeight)
	if err != nil {
		return nil, fmt.Errorf("mysql: confirm notes list: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var position sql.NullInt64
		var divIdx sql.NullInt64
		var memo sql.NullString
		var spentHeight sql.NullInt64
		var spentTxid sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&spentHeight,
			&spentTxid,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("mysql: confirm notes list: %w", err)
		}
		if position.Valid {
			n.Position = &position.Int64
		}
		if divIdx.Valid {
			n.DiversifierIndex = uint32(divIdx.Int64)
		}
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if spentHeight.Valid {
			n.SpentHeight = &spentHeight.Int64
		}
		if spentTxid.Valid {
			n.SpentTxID = &spentTxid.String
		}
		if confirmedHeight.Valid {
			n.ConfirmedHeight = &confirmedHeight.Int64
		}
		if spentConfirmedHeight.Valid {
			n.SpentConfirmedHeight = &spentConfirmedHeight.Int64
		}
		n.ConfirmedHeight = &confirmationHeight
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: confirm notes list: %w", err)
	}

	if len(out) == 0 {
		return nil, nil
	}

	if _, err := t.tx.ExecContext(ctx, `
UPDATE notes
SET confirmed_height = ?
WHERE confirmed_height IS NULL AND height <= ?
`, confirmationHeight, maxNoteHeight); err != nil {
		return nil, fmt.Errorf("mysql: confirm notes update: %w", err)
	}

	return out, nil
}

func (t *myTx) ConfirmSpends(ctx context.Context, confirmationHeight int64, maxSpentHeight int64) ([]store.Note, error) {
	rows, err := t.tx.QueryContext(ctx, `
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE spent_height IS NOT NULL AND spent_confirmed_height IS NULL AND spent_height <= ?
ORDER BY spent_height, wallet_id, txid, action_index
`, maxSpentHeight)
	if err != nil {
		return nil, fmt.Errorf("mysql: confirm spends list: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var position sql.NullInt64
		var divIdx sql.NullInt64
		var memo sql.NullString
		var spentHeight sql.NullInt64
		var spentTxid sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
		if err := rows.Scan(
			&n.WalletID,
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&position,
			&divIdx,
			&n.RecipientAddress,
			&n.ValueZat,
			&memo,
			&n.NoteNullifier,
			&spentHeight,
			&spentTxid,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("mysql: confirm spends list: %w", err)
		}
		if position.Valid {
			n.Position = &position.Int64
		}
		if divIdx.Valid {
			n.DiversifierIndex = uint32(divIdx.Int64)
		}
		if memo.Valid {
			n.MemoHex = &memo.String
		}
		if spentHeight.Valid {
			n.SpentHeight = &spentHeight.Int64
		}
		if spentTxid.Valid {
			n.SpentTxID = &spentTxid.String
		}
		if confirmedHeight.Valid {
			n.ConfirmedHeight = &confirmedHeight.Int64
		}
		if spentConfirmedHeight.Valid {
			n.SpentConfirmedHeight = &spentConfirmedHeight.Int64
		}
		n.SpentConfirmedHeight = &confirmationHeight
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: confirm spends list: %w", err)
	}

	if len(out) == 0 {
		return nil, nil
	}

	if _, err := t.tx.ExecContext(ctx, `
UPDATE notes
SET spent_confirmed_height = ?
WHERE spent_height IS NOT NULL AND spent_confirmed_height IS NULL AND spent_height <= ?
`, confirmationHeight, maxSpentHeight); err != nil {
		return nil, fmt.Errorf("mysql: confirm spends update: %w", err)
	}

	return out, nil
}

func (t *myTx) InsertEvent(ctx context.Context, e store.Event) error {
	_, err := t.tx.ExecContext(ctx, `
INSERT INTO events (kind, wallet_id, height, payload)
VALUES (?, ?, ?, ?)
`, e.Kind, e.WalletID, e.Height, string(e.Payload))
	if err != nil {
		return fmt.Errorf("mysql: insert event: %w", err)
	}
	return nil
}
