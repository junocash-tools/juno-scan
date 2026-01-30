package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/db/migrate"
	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-sdk-go/types"
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

		var orphanDeposits []store.Note
		var unconfirmedDeposits []store.Note
		var orphanSpends []store.Note
		var unconfirmedSpends []store.Note
		var orphanOutgoingOutputs []store.OutgoingOutput
		var unconfirmedOutgoingOutputs []store.OutgoingOutput

		if height >= 0 {
			fetchNotes := func(query string, args ...any) ([]store.Note, error) {
				rows, err := pgtx.tx.Query(ctx, query, args...)
				if err != nil {
					return nil, err
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
						return nil, err
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
					return nil, err
				}
				return out, nil
			}

			baseSelect := `
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
`

			var err error

			orphanDeposits, err = fetchNotes(baseSelect+`WHERE height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list orphan deposits: %w", err)
			}

			unconfirmedDeposits, err = fetchNotes(baseSelect+`WHERE height <= $1 AND confirmed_height IS NOT NULL AND confirmed_height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list unconfirmed deposits: %w", err)
			}

			orphanSpends, err = fetchNotes(baseSelect+`WHERE height <= $1 AND spent_height IS NOT NULL AND spent_height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list orphan spends: %w", err)
			}

			unconfirmedSpends, err = fetchNotes(baseSelect+`WHERE height <= $1 AND spent_height IS NOT NULL AND spent_height <= $1 AND spent_confirmed_height IS NOT NULL AND spent_confirmed_height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list unconfirmed spends: %w", err)
			}

			fetchOutgoingOutputs := func(query string, args ...any) ([]store.OutgoingOutput, error) {
				rows, err := pgtx.tx.Query(ctx, query, args...)
				if err != nil {
					return nil, err
				}
				defer rows.Close()

				var out []store.OutgoingOutput
				for rows.Next() {
					var o store.OutgoingOutput
					var minedHeight sql.NullInt64
					var confirmedHeight sql.NullInt64
					var mempoolSeenAt sql.NullTime
					var memo sql.NullString
					var recipientScope sql.NullString
					if err := rows.Scan(
						&o.WalletID,
						&o.TxID,
						&o.ActionIndex,
						&minedHeight,
						&confirmedHeight,
						&mempoolSeenAt,
						&o.RecipientAddress,
						&o.ValueZat,
						&memo,
						&o.OvkScope,
						&recipientScope,
						&o.CreatedAt,
					); err != nil {
						return nil, err
					}
					if minedHeight.Valid {
						o.MinedHeight = &minedHeight.Int64
					}
					if confirmedHeight.Valid {
						o.ConfirmedHeight = &confirmedHeight.Int64
					}
					if mempoolSeenAt.Valid {
						t := mempoolSeenAt.Time.UTC()
						o.MempoolSeenAt = &t
					}
					if memo.Valid {
						o.MemoHex = &memo.String
					}
					if recipientScope.Valid {
						o.RecipientScope = &recipientScope.String
					}
					out = append(out, o)
				}
				if err := rows.Err(); err != nil {
					return nil, err
				}
				return out, nil
			}

			outgoingBaseSelect := `
SELECT wallet_id, txid, action_index, mined_height, confirmed_height, mempool_seen_at, recipient_address, value_zat, memo_hex, ovk_scope, recipient_scope, created_at
FROM outgoing_outputs
`

			orphanOutgoingOutputs, err = fetchOutgoingOutputs(outgoingBaseSelect+`WHERE mined_height IS NOT NULL AND mined_height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list orphan outgoing outputs: %w", err)
			}

			unconfirmedOutgoingOutputs, err = fetchOutgoingOutputs(outgoingBaseSelect+`WHERE mined_height IS NOT NULL AND mined_height <= $1 AND confirmed_height IS NOT NULL AND confirmed_height > $1`, height)
			if err != nil {
				return fmt.Errorf("postgres: rollback list unconfirmed outgoing outputs: %w", err)
			}
		}

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
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM outgoing_outputs WHERE mined_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback outgoing outputs: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET spent_height = NULL, spent_txid = NULL, spent_confirmed_height = NULL WHERE spent_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unspend: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET confirmed_height = NULL WHERE confirmed_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unconfirm: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE outgoing_outputs SET confirmed_height = NULL WHERE confirmed_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unconfirm outgoing outputs: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `UPDATE notes SET spent_confirmed_height = NULL WHERE spent_confirmed_height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback unconfirm spend: %w", err)
		}
		if _, err := pgtx.tx.Exec(ctx, `DELETE FROM blocks WHERE height > $1`, height); err != nil {
			return fmt.Errorf("postgres: rollback blocks: %w", err)
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
					return fmt.Errorf("postgres: rollback marshal deposit orphaned: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
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
					return fmt.Errorf("postgres: rollback marshal deposit unconfirmed: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
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
					return fmt.Errorf("postgres: rollback marshal spend orphaned: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
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
					return fmt.Errorf("postgres: rollback marshal spend unconfirmed: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
					Kind:     events.KindSpendUnconfirmed,
					WalletID: n.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}

			for _, o := range orphanOutgoingOutputs {
				if o.MinedHeight == nil {
					continue
				}

				memoHex := ""
				if o.MemoHex != nil {
					memoHex = *o.MemoHex
				}

				heightCopy := *o.MinedHeight
				payload := events.OutgoingOutputOrphanedPayload{
					OutgoingOutputEventPayload: events.OutgoingOutputEventPayload{
						Version:          types.V1,
						WalletID:         o.WalletID,
						TxID:             o.TxID,
						Height:           &heightCopy,
						ActionIndex:      uint32(o.ActionIndex),
						AmountZatoshis:   uint64(o.ValueZat),
						RecipientAddress: o.RecipientAddress,
						MemoHex:          memoHex,
						OvkScope:         o.OvkScope,
						RecipientScope:   derefString(o.RecipientScope),
						Status: types.TxStatus{
							State:  types.TxStateOrphaned,
							Height: *o.MinedHeight,
						},
					},
					OrphanedAtHeight: height,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("postgres: rollback marshal outgoing output orphaned: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
					Kind:     events.KindOutgoingOutputOrphaned,
					WalletID: o.WalletID,
					Height:   height,
					Payload:  b,
				}); err != nil {
					return err
				}
			}

			for _, o := range unconfirmedOutgoingOutputs {
				if o.MinedHeight == nil || o.ConfirmedHeight == nil {
					continue
				}

				memoHex := ""
				if o.MemoHex != nil {
					memoHex = *o.MemoHex
				}

				confirmations := height - *o.MinedHeight + 1
				if confirmations < 0 {
					confirmations = 0
				}

				heightCopy := *o.MinedHeight
				payload := events.OutgoingOutputUnconfirmedPayload{
					OutgoingOutputEventPayload: events.OutgoingOutputEventPayload{
						Version:          types.V1,
						WalletID:         o.WalletID,
						TxID:             o.TxID,
						Height:           &heightCopy,
						ActionIndex:      uint32(o.ActionIndex),
						AmountZatoshis:   uint64(o.ValueZat),
						RecipientAddress: o.RecipientAddress,
						MemoHex:          memoHex,
						OvkScope:         o.OvkScope,
						RecipientScope:   derefString(o.RecipientScope),
						Status: types.TxStatus{
							State:         types.TxStateConfirmed,
							Height:        *o.MinedHeight,
							Confirmations: confirmations,
						},
					},
					RollbackHeight:          height,
					PreviousConfirmedHeight: *o.ConfirmedHeight,
				}
				b, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("postgres: rollback marshal outgoing output unconfirmed: %w", err)
				}
				if err := pgtx.InsertEvent(ctx, store.Event{
					Kind:     events.KindOutgoingOutputUnconfirmed,
					WalletID: o.WalletID,
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

func (s *Store) ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int, filter store.EventFilter) ([]store.Event, int64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if filter.BlockHeight != nil && *filter.BlockHeight < 0 {
		return nil, afterID, errors.New("postgres: negative blockHeight")
	}

	query := `
SELECT id, kind, height, payload, created_at
FROM events
WHERE wallet_id = $1 AND id > $2
`
	args := []any{walletID, afterID}
	argN := 3
	if filter.BlockHeight != nil {
		query += fmt.Sprintf(" AND height = $%d\n", argN)
		args = append(args, *filter.BlockHeight)
		argN++
	}
	if len(filter.Kinds) > 0 {
		query += fmt.Sprintf(" AND kind = ANY($%d::text[])\n", argN)
		args = append(args, filter.Kinds)
		argN++
	}
	if strings.TrimSpace(filter.TxID) != "" {
		query += fmt.Sprintf(" AND payload->>'txid' = $%d\n", argN)
		args = append(args, strings.ToLower(strings.TrimSpace(filter.TxID)))
		argN++
	}
	query += fmt.Sprintf("ORDER BY id\nLIMIT $%d\n", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
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
SELECT txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, pending_spent_txid, pending_spent_at, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
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
			&n.PendingSpentTxID,
			&n.PendingSpentAt,
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

func (s *Store) UpdatePendingSpends(ctx context.Context, pending map[string]string, seenAt time.Time) error {
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}

	nullifiers := make([]string, 0, len(pending))
	txids := make([]string, 0, len(pending))
	for nf, txid := range pending {
		nf = strings.TrimSpace(nf)
		txid = strings.TrimSpace(txid)
		if nf == "" || txid == "" {
			continue
		}
		nullifiers = append(nullifiers, nf)
		txids = append(txids, txid)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if len(nullifiers) == 0 {
		if _, err := tx.Exec(ctx, `
UPDATE notes
SET pending_spent_txid = NULL, pending_spent_at = NULL
WHERE spent_height IS NULL AND pending_spent_txid IS NOT NULL
`); err != nil {
			return fmt.Errorf("postgres: clear pending spends: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE notes
SET pending_spent_txid = NULL, pending_spent_at = NULL
WHERE spent_height IS NULL
  AND pending_spent_txid IS NOT NULL
  AND NOT (note_nullifier = ANY($1::text[]))
`, nullifiers); err != nil {
			return fmt.Errorf("postgres: clear pending spends: %w", err)
		}

		if _, err := tx.Exec(ctx, `
WITH pending (note_nullifier, pending_spent_txid) AS (SELECT * FROM UNNEST($1::text[], $2::text[]))
UPDATE notes n
SET pending_spent_txid = pending.pending_spent_txid,
    pending_spent_at = CASE
      WHEN n.pending_spent_txid IS DISTINCT FROM pending.pending_spent_txid THEN $3
      ELSE COALESCE(n.pending_spent_at, $3)
    END
FROM pending
WHERE n.spent_height IS NULL AND n.note_nullifier = pending.note_nullifier
`, nullifiers, txids, seenAt); err != nil {
			return fmt.Errorf("postgres: set pending spends: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

func (s *Store) ListNotesByPendingSpentTxIDs(ctx context.Context, txids []string) ([]store.Note, error) {
	txids = cleanStringsLower(txids)
	if len(txids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, `
SELECT wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier, pending_spent_txid, pending_spent_at, spent_height, spent_txid, confirmed_height, spent_confirmed_height, created_at
FROM notes
WHERE spent_height IS NULL AND pending_spent_txid = ANY($1::text[])
ORDER BY wallet_id, txid, action_index
`, txids)
	if err != nil {
		return nil, fmt.Errorf("postgres: list pending spend notes: %w", err)
	}
	defer rows.Close()

	var out []store.Note
	for rows.Next() {
		var n store.Note
		var divIdx int64
		var memo sql.NullString
		var confirmedHeight sql.NullInt64
		var spentConfirmedHeight sql.NullInt64
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
			&n.PendingSpentTxID,
			&n.PendingSpentAt,
			&n.SpentHeight,
			&n.SpentTxID,
			&confirmedHeight,
			&spentConfirmedHeight,
			&n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: list pending spend notes: %w", err)
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
		return nil, fmt.Errorf("postgres: list pending spend notes: %w", err)
	}
	return out, nil
}

func (s *Store) ListOutgoingOutputsByTxID(ctx context.Context, walletID, txid string) ([]store.OutgoingOutput, error) {
	walletID = strings.TrimSpace(walletID)
	txid = strings.ToLower(strings.TrimSpace(txid))
	if walletID == "" || txid == "" {
		return nil, errors.New("postgres: wallet_id and txid are required")
	}

	rows, err := s.pool.Query(ctx, `
SELECT txid, action_index, mined_height, confirmed_height, mempool_seen_at, recipient_address, value_zat, memo_hex, ovk_scope, recipient_scope, created_at
FROM outgoing_outputs
WHERE wallet_id = $1 AND txid = $2
ORDER BY action_index
`, walletID, txid)
	if err != nil {
		return nil, fmt.Errorf("postgres: list outgoing outputs: %w", err)
	}
	defer rows.Close()

	var out []store.OutgoingOutput
	for rows.Next() {
		var o store.OutgoingOutput
		var minedHeight sql.NullInt64
		var confirmedHeight sql.NullInt64
		var mempoolSeenAt sql.NullTime
		var memo sql.NullString
		var recipientScope sql.NullString
		o.WalletID = walletID
		if err := rows.Scan(
			&o.TxID,
			&o.ActionIndex,
			&minedHeight,
			&confirmedHeight,
			&mempoolSeenAt,
			&o.RecipientAddress,
			&o.ValueZat,
			&memo,
			&o.OvkScope,
			&recipientScope,
			&o.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: list outgoing outputs: %w", err)
		}
		if minedHeight.Valid {
			o.MinedHeight = &minedHeight.Int64
		}
		if confirmedHeight.Valid {
			o.ConfirmedHeight = &confirmedHeight.Int64
		}
		if mempoolSeenAt.Valid {
			t := mempoolSeenAt.Time.UTC()
			o.MempoolSeenAt = &t
		}
		if memo.Valid {
			o.MemoHex = &memo.String
		}
		if recipientScope.Valid {
			o.RecipientScope = &recipientScope.String
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list outgoing outputs: %w", err)
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

func (s *Store) FirstOrchardCommitmentPositionFromHeight(ctx context.Context, height int64) (int64, bool, error) {
	var pos sql.NullInt64
	if err := s.pool.QueryRow(ctx, `SELECT MIN(position) FROM orchard_commitments WHERE height >= $1`, height).Scan(&pos); err != nil {
		return 0, false, fmt.Errorf("postgres: first commitment position: %w", err)
	}
	if !pos.Valid {
		return 0, false, nil
	}
	return pos.Int64, true, nil
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
SET spent_height = $1,
    spent_txid = $2,
    spent_confirmed_height = NULL,
    pending_spent_txid = NULL,
    pending_spent_at = NULL
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

func (t *pgTx) InsertNote(ctx context.Context, n store.Note) (bool, error) {
	var inserted int
	err := t.tx.QueryRow(ctx, `
INSERT INTO notes (
  wallet_id, txid, action_index, height, position, diversifier_index, recipient_address, value_zat, memo_hex, note_nullifier
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (wallet_id, txid, action_index) DO NOTHING
RETURNING 1
`, n.WalletID, n.TxID, n.ActionIndex, n.Height, n.Position, int64(n.DiversifierIndex), n.RecipientAddress, n.ValueZat, n.MemoHex, n.NoteNullifier).Scan(&inserted)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("postgres: insert note: %w", err)
}

func (t *pgTx) InsertOutgoingOutput(ctx context.Context, o store.OutgoingOutput) (bool, error) {
	o.WalletID = strings.TrimSpace(o.WalletID)
	o.TxID = strings.ToLower(strings.TrimSpace(o.TxID))
	o.OvkScope = strings.ToLower(strings.TrimSpace(o.OvkScope))
	if o.RecipientScope != nil {
		v := strings.ToLower(strings.TrimSpace(*o.RecipientScope))
		if v == "" {
			o.RecipientScope = nil
		} else {
			o.RecipientScope = &v
		}
	}
	if o.WalletID == "" || o.TxID == "" || o.OvkScope == "" || o.RecipientAddress == "" {
		return false, errors.New("postgres: outgoing output is missing required fields")
	}

	var memo sql.NullString
	if o.MemoHex != nil && strings.TrimSpace(*o.MemoHex) != "" {
		memo = sql.NullString{String: strings.TrimSpace(*o.MemoHex), Valid: true}
	}

	var minedHeight sql.NullInt64
	if o.MinedHeight != nil {
		minedHeight = sql.NullInt64{Int64: *o.MinedHeight, Valid: true}
	}

	var mempoolSeenAt sql.NullTime
	if o.MempoolSeenAt != nil && !o.MempoolSeenAt.IsZero() {
		mempoolSeenAt = sql.NullTime{Time: o.MempoolSeenAt.UTC(), Valid: true}
	}

	var recipientScope sql.NullString
	if o.RecipientScope != nil && strings.TrimSpace(*o.RecipientScope) != "" {
		recipientScope = sql.NullString{String: strings.TrimSpace(*o.RecipientScope), Valid: true}
	}

	var inserted int
	err := t.tx.QueryRow(ctx, `
INSERT INTO outgoing_outputs (
  wallet_id, txid, action_index, mined_height, mempool_seen_at,
  recipient_address, value_zat, memo_hex, ovk_scope, recipient_scope
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (wallet_id, txid, action_index) DO NOTHING
RETURNING 1
`, o.WalletID, o.TxID, o.ActionIndex, minedHeight, mempoolSeenAt, o.RecipientAddress, o.ValueZat, memo, o.OvkScope, recipientScope).Scan(&inserted)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("postgres: insert outgoing output: %w", err)
	}

	tag, err := t.tx.Exec(ctx, `
UPDATE outgoing_outputs
SET mined_height = COALESCE(outgoing_outputs.mined_height, $4::bigint),
    mempool_seen_at = COALESCE(outgoing_outputs.mempool_seen_at, $5::timestamptz)
WHERE wallet_id = $1 AND txid = $2 AND action_index = $3
  AND (
    (outgoing_outputs.mined_height IS NULL AND $4::bigint IS NOT NULL) OR
    (outgoing_outputs.mempool_seen_at IS NULL AND $5::timestamptz IS NOT NULL)
  )
`, o.WalletID, o.TxID, o.ActionIndex, minedHeight, mempoolSeenAt)
	if err != nil {
		return false, fmt.Errorf("postgres: update outgoing output: %w", err)
	}
	return tag.RowsAffected() > 0, nil
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

func (t *pgTx) ConfirmOutgoingOutputs(ctx context.Context, confirmationHeight int64, maxMinedHeight int64) ([]store.OutgoingOutput, error) {
	rows, err := t.tx.Query(ctx, `
UPDATE outgoing_outputs
SET confirmed_height = $1
WHERE mined_height IS NOT NULL AND confirmed_height IS NULL AND mined_height <= $2
RETURNING wallet_id, txid, action_index, mined_height, confirmed_height, mempool_seen_at, recipient_address, value_zat, memo_hex, ovk_scope, recipient_scope, created_at
`, confirmationHeight, maxMinedHeight)
	if err != nil {
		return nil, fmt.Errorf("postgres: confirm outgoing outputs: %w", err)
	}
	defer rows.Close()

	var out []store.OutgoingOutput
	for rows.Next() {
		var o store.OutgoingOutput
		var minedHeight sql.NullInt64
		var confirmedHeight sql.NullInt64
		var mempoolSeenAt sql.NullTime
		var memo sql.NullString
		var recipientScope sql.NullString
		if err := rows.Scan(
			&o.WalletID,
			&o.TxID,
			&o.ActionIndex,
			&minedHeight,
			&confirmedHeight,
			&mempoolSeenAt,
			&o.RecipientAddress,
			&o.ValueZat,
			&memo,
			&o.OvkScope,
			&recipientScope,
			&o.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: confirm outgoing outputs: %w", err)
		}
		if minedHeight.Valid {
			o.MinedHeight = &minedHeight.Int64
		}
		if confirmedHeight.Valid {
			o.ConfirmedHeight = &confirmedHeight.Int64
		}
		if mempoolSeenAt.Valid {
			tm := mempoolSeenAt.Time.UTC()
			o.MempoolSeenAt = &tm
		}
		if memo.Valid {
			o.MemoHex = &memo.String
		}
		if recipientScope.Valid {
			o.RecipientScope = &recipientScope.String
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: confirm outgoing outputs: %w", err)
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

func cleanStringsLower(v []string) []string {
	seen := make(map[string]struct{}, len(v))
	out := make([]string, 0, len(v))
	for _, s := range v {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
