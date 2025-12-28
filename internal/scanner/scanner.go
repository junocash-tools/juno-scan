package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

type Scanner struct {
	st    store.Store
	rpc   *sdkjunocashd.Client
	uaHRP string

	pollInterval time.Duration
}

func New(st store.Store, rpc *sdkjunocashd.Client, uaHRP string, pollInterval time.Duration) (*Scanner, error) {
	if st == nil {
		return nil, errors.New("scanner: store is nil")
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
		st:           st,
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
	tip, ok, err := s.st.Tip(ctx)
	if err != nil {
		return err
	}

	nextHeight := int64(0)
	if ok {
		nextHeight = tip.Height + 1
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
	if ok && tip.Height >= 0 {
		daemonTipHash, err := s.rpc.GetBlockHash(ctx, tip.Height)
		if err == nil && daemonTipHash != tip.Hash {
			common, err := s.findCommonAncestor(ctx, tip.Height)
			if err != nil {
				return err
			}
			log.Printf("reorg detected: rolling back to height %d", common)
			if err := s.st.RollbackToHeight(ctx, common); err != nil {
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
		dbHash, ok, err := s.st.HashAtHeight(ctx, h)
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

func (s *Scanner) processBlock(ctx context.Context, blk blockVerbose2) error {
	wallets, err := s.loadWallets(ctx)
	if err != nil {
		return err
	}

	return s.st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   blk.Height,
			Hash:     blk.Hash,
			PrevHash: blk.PreviousBlockHash,
			Time:     blk.Time,
		}); err != nil {
			return err
		}

		nextPos, err := tx.NextOrchardCommitmentPosition(ctx)
		if err != nil {
			return err
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
				if err := tx.InsertOrchardAction(ctx, store.OrchardAction{
					Height:          blk.Height,
					TxID:            t.TxID,
					ActionIndex:     int32(a.ActionIndex),
					ActionNullifier: a.ActionNullifier,
					CMX:             a.CMX,
					EphemeralKey:    a.EphemeralKey,
					EncCiphertext:   a.EncCiphertext,
				}); err != nil {
					return err
				}

				pos := nextPos
				nextPos++
				posByTxAction[t.TxID][a.ActionIndex] = pos

				if err := tx.InsertOrchardCommitment(ctx, store.OrchardCommitment{
					Position:    pos,
					Height:      blk.Height,
					TxID:        t.TxID,
					ActionIndex: int32(a.ActionIndex),
					CMX:         a.CMX,
				}); err != nil {
					return err
				}
			}

			// Mark spends.
			nullifiers := make([]string, 0, len(res.Actions))
			for _, a := range res.Actions {
				nullifiers = append(nullifiers, a.ActionNullifier)
			}
			if err := tx.MarkNotesSpent(ctx, blk.Height, t.TxID, nullifiers); err != nil {
				return err
			}

			// Record deposits.
			for _, n := range res.Notes {
				pos := posByTxAction[t.TxID][n.ActionIndex]
				posCopy := pos
				if err := tx.InsertNote(ctx, store.Note{
					WalletID:         n.WalletID,
					TxID:             t.TxID,
					ActionIndex:      int32(n.ActionIndex),
					Height:           blk.Height,
					Position:         &posCopy,
					RecipientAddress: n.RecipientAddress,
					ValueZat:         int64(n.ValueZat),
					NoteNullifier:    n.NoteNullifier,
				}); err != nil {
					return err
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
				if err := tx.InsertEvent(ctx, store.Event{
					Kind:     "deposit",
					WalletID: n.WalletID,
					Height:   blk.Height,
					Payload:  payloadBytes,
				}); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *Scanner) loadWallets(ctx context.Context) ([]orchardscan.Wallet, error) {
	wallets, err := s.st.ListEnabledWalletUFVKs(ctx)
	if err != nil {
		return nil, fmt.Errorf("scanner: list wallets: %w", err)
	}
	out := make([]orchardscan.Wallet, 0, len(wallets))
	for _, w := range wallets {
		out = append(out, orchardscan.Wallet{
			WalletID: w.WalletID,
			UFVK:     w.UFVK,
		})
	}
	return out, nil
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
