package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scanner struct {
	db    *pgxpool.Pool
	rpc   *sdkjunocashd.Client
	uaHRP string

	pollInterval time.Duration
}

func New(db *pgxpool.Pool, rpc *sdkjunocashd.Client, uaHRP string, pollInterval time.Duration) (*Scanner, error) {
	if db == nil {
		return nil, errors.New("scanner: db is nil")
	}
	if rpc == nil {
		return nil, errors.New("scanner: rpc is nil")
	}
	if uaHRP == "" {
		return nil, errors.New("scanner: ua hrp is required")
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &Scanner{
		db:           db,
		rpc:          rpc,
		uaHRP:        uaHRP,
		pollInterval: pollInterval,
	}, nil
}

func (s *Scanner) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if err := s.scanOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Scanner) scanOnce(ctx context.Context) error {
	tipHeight, tipHash, ok, err := s.dbTip(ctx)
	if err != nil {
		return err
	}

	nextHeight := int64(0)
	if ok {
		nextHeight = tipHeight + 1
	}

	chainHeight, err := s.rpc.GetBlockCount(ctx)
	if err != nil {
		return fmt.Errorf("scanner: getblockcount: %w", err)
	}
	if nextHeight > chainHeight {
		return nil
	}

	nextHash, err := s.rpc.GetBlockHash(ctx, nextHeight)
	if err != nil {
		return fmt.Errorf("scanner: getblockhash(%d): %w", nextHeight, err)
	}

	// Reorg detection: compare stored tip with daemon chain at the same height.
	if ok && tipHeight >= 0 {
		daemonTipHash, err := s.rpc.GetBlockHash(ctx, tipHeight)
		if err == nil && daemonTipHash != tipHash {
			common, err := s.findCommonAncestor(ctx, tipHeight)
			if err != nil {
				return err
			}
			log.Printf("reorg detected: rolling back to height %d", common)
			if err := s.rollbackToHeight(ctx, common); err != nil {
				return err
			}
			return nil
		}
	}

	var blk blockVerbose2
	if err := s.rpc.Call(ctx, "getblock", []any{nextHash, 2}, &blk); err != nil {
		return fmt.Errorf("scanner: getblock(%d): %w", nextHeight, err)
	}
	if blk.Height != nextHeight {
		return fmt.Errorf("scanner: daemon returned unexpected height: got %d want %d", blk.Height, nextHeight)
	}

	return s.processBlock(ctx, blk)
}

func (s *Scanner) findCommonAncestor(ctx context.Context, fromHeight int64) (int64, error) {
	h := fromHeight
	for h >= 0 {
		dbHash, ok, err := s.dbHashAtHeight(ctx, h)
		if err != nil {
			return 0, err
		}
		if !ok {
			h--
			continue
		}
		chainHash, err := s.rpc.GetBlockHash(ctx, h)
		if err == nil && chainHash == dbHash {
			return h, nil
		}
		h--
	}
	return -1, nil
}

func (s *Scanner) rollbackToHeight(ctx context.Context, height int64) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("scanner: rollback begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM events WHERE height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback events: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM orchard_actions WHERE height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback actions: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM orchard_commitments WHERE height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback commitments: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM notes WHERE height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback notes: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE notes SET spent_height = NULL, spent_txid = NULL WHERE spent_height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback unspend: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM blocks WHERE height > $1`, height); err != nil {
		return fmt.Errorf("scanner: rollback blocks: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("scanner: rollback commit: %w", err)
	}
	return nil
}

func (s *Scanner) processBlock(ctx context.Context, blk blockVerbose2) error {
	wallets, err := s.loadWallets(ctx)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("scanner: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
INSERT INTO blocks (height, hash, prev_hash, time)
VALUES ($1, $2, $3, $4)
ON CONFLICT (height) DO NOTHING
`, blk.Height, blk.Hash, blk.PreviousBlockHash, blk.Time); err != nil {
		return fmt.Errorf("scanner: insert block: %w", err)
	}

	var nextPos int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(position) + 1, 0) FROM orchard_commitments`).Scan(&nextPos); err != nil {
		return fmt.Errorf("scanner: next position: %w", err)
	}

	posByTxAction := make(map[string]map[uint32]int64)

	for _, t := range blk.Tx {
		res, err := orchardscan.ScanTx(ctx, s.uaHRP, wallets, t.Hex)
		if err != nil {
			return fmt.Errorf("scanner: scan tx %s: %w", t.TxID, err)
		}

		if len(res.Actions) == 0 {
			continue
		}

		if _, ok := posByTxAction[t.TxID]; !ok {
			posByTxAction[t.TxID] = make(map[uint32]int64, len(res.Actions))
		}

		for _, a := range res.Actions {
			if _, err := tx.Exec(ctx, `
INSERT INTO orchard_actions (height, txid, action_index, action_nullifier, cmx, ephemeral_key, enc_ciphertext)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (txid, action_index) DO NOTHING
`, blk.Height, t.TxID, int32(a.ActionIndex), a.ActionNullifier, a.CMX, a.EphemeralKey, a.EncCiphertext); err != nil {
				return fmt.Errorf("scanner: insert action: %w", err)
			}

			pos := nextPos
			nextPos++
			posByTxAction[t.TxID][a.ActionIndex] = pos

			if _, err := tx.Exec(ctx, `
INSERT INTO orchard_commitments (position, height, txid, action_index, cmx)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (position) DO NOTHING
`, pos, blk.Height, t.TxID, int32(a.ActionIndex), a.CMX); err != nil {
				return fmt.Errorf("scanner: insert commitment: %w", err)
			}
		}

		// Mark spends.
		nullifiers := make([]string, 0, len(res.Actions))
		for _, a := range res.Actions {
			nullifiers = append(nullifiers, a.ActionNullifier)
		}
		if _, err := tx.Exec(ctx, `
UPDATE notes
SET spent_height = $1, spent_txid = $2
WHERE spent_height IS NULL AND note_nullifier = ANY($3::text[])
`, blk.Height, t.TxID, nullifiers); err != nil {
			return fmt.Errorf("scanner: mark spent: %w", err)
		}

		// Record deposits.
		for _, n := range res.Notes {
			pos := posByTxAction[t.TxID][n.ActionIndex]
			if _, err := tx.Exec(ctx, `
INSERT INTO notes (
  wallet_id, txid, action_index, height, position, recipient_address, value_zat, note_nullifier
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (wallet_id, txid, action_index) DO NOTHING
`, n.WalletID, t.TxID, int32(n.ActionIndex), blk.Height, pos, n.RecipientAddress, int64(n.ValueZat), n.NoteNullifier); err != nil {
				return fmt.Errorf("scanner: insert note: %w", err)
			}

			payload := map[string]any{
				"version":           "v1",
				"wallet_id":         n.WalletID,
				"diversifier_index": n.DiversifierIndex,
				"recipient_address": n.RecipientAddress,
				"txid":              t.TxID,
				"height":            blk.Height,
				"action_index":      n.ActionIndex,
				"amount_zatoshis":   n.ValueZat,
				"note_nullifier":    n.NoteNullifier,
			}
			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("scanner: marshal event payload: %w", err)
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO events (kind, wallet_id, height, payload)
VALUES ($1, $2, $3, $4::jsonb)
`, "deposit", n.WalletID, blk.Height, string(payloadBytes)); err != nil {
				return fmt.Errorf("scanner: insert event: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("scanner: commit: %w", err)
	}
	return nil
}

func (s *Scanner) loadWallets(ctx context.Context) ([]orchardscan.Wallet, error) {
	rows, err := s.db.Query(ctx, `SELECT wallet_id, ufvk FROM wallets WHERE disabled_at IS NULL ORDER BY wallet_id`)
	if err != nil {
		return nil, fmt.Errorf("scanner: list wallets: %w", err)
	}
	defer rows.Close()

	var out []orchardscan.Wallet
	for rows.Next() {
		var w orchardscan.Wallet
		if err := rows.Scan(&w.WalletID, &w.UFVK); err != nil {
			return nil, fmt.Errorf("scanner: scan wallet: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanner: list wallets: %w", err)
	}
	return out, nil
}

func (s *Scanner) dbTip(ctx context.Context) (height int64, hash string, ok bool, err error) {
	if err := s.db.QueryRow(ctx, `SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1`).Scan(&height, &hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", false, nil
		}
		return 0, "", false, fmt.Errorf("scanner: tip: %w", err)
	}
	return height, hash, true, nil
}

func (s *Scanner) dbHashAtHeight(ctx context.Context, height int64) (hash string, ok bool, err error) {
	if err := s.db.QueryRow(ctx, `SELECT hash FROM blocks WHERE height=$1`, height).Scan(&hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("scanner: hash at height %d: %w", height, err)
	}
	return hash, true, nil
}

type blockVerbose2 struct {
	Hash              string       `json:"hash"`
	Height            int64        `json:"height"`
	Time              int64        `json:"time"`
	PreviousBlockHash string       `json:"previousblockhash,omitempty"`
	Tx                []txVerbose2 `json:"tx"`
}

type txVerbose2 struct {
	TxID string `json:"txid"`
	Hex  string `json:"hex"`
}
