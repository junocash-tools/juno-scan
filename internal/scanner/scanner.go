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
	"github.com/Abdullah1738/juno-sdk-go/types"
)

type Scanner struct {
	st    store.Store
	rpc   *sdkjunocashd.Client
	uaHRP string

	pollInterval  time.Duration
	confirmations int64
}

func New(st store.Store, rpc *sdkjunocashd.Client, uaHRP string, pollInterval time.Duration, confirmations int64) (*Scanner, error) {
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
	if confirmations <= 0 {
		confirmations = 100
	}
	return &Scanner{
		st:            st,
		rpc:           rpc,
		uaHRP:         uaHRP,
		pollInterval:  pollInterval,
		confirmations: confirmations,
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
			spentNotes, err := tx.MarkNotesSpent(ctx, blk.Height, t.TxID, nullifiers)
			if err != nil {
				return err
			}
			for _, n := range spentNotes {
				payload := spendEventPayload{
					Version:          types.V1,
					WalletID:         n.WalletID,
					DiversifierIndex: n.DiversifierIndex,
					TxID:             t.TxID,
					Height:           blk.Height,
					NoteTxID:         n.TxID,
					NoteActionIndex:  uint32(n.ActionIndex),
					NoteHeight:       n.Height,
					AmountZatoshis:   uint64(n.ValueZat),
					NoteNullifier:    n.NoteNullifier,
					RecipientAddress: n.RecipientAddress,
					Status: types.TxStatus{
						State:         types.TxStateConfirmed,
						Height:        blk.Height,
						Confirmations: 1,
					},
				}
				payloadBytes, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("scanner: marshal spend payload: %w", err)
				}
				if err := tx.InsertEvent(ctx, store.Event{
					Kind:     "SpendEvent",
					WalletID: n.WalletID,
					Height:   blk.Height,
					Payload:  payloadBytes,
				}); err != nil {
					return err
				}
			}

			// Record deposits.
			for _, n := range res.Notes {
				pos := posByTxAction[t.TxID][n.ActionIndex]
				posCopy := pos
				var memoHexPtr *string
				if n.MemoHex != "" {
					memo := n.MemoHex
					memoHexPtr = &memo
				}
				if err := tx.InsertNote(ctx, store.Note{
					WalletID:         n.WalletID,
					TxID:             t.TxID,
					ActionIndex:      int32(n.ActionIndex),
					Height:           blk.Height,
					Position:         &posCopy,
					DiversifierIndex: n.DiversifierIndex,
					RecipientAddress: n.RecipientAddress,
					ValueZat:         int64(n.ValueZat),
					MemoHex:          memoHexPtr,
					NoteNullifier:    n.NoteNullifier,
				}); err != nil {
					return err
				}

				payload := depositEventPayload{
					DepositEvent: types.DepositEvent{
						Version:          types.V1,
						WalletID:         n.WalletID,
						DiversifierIndex: n.DiversifierIndex,
						TxID:             t.TxID,
						Height:           blk.Height,
						ActionIndex:      n.ActionIndex,
						AmountZatoshis:   n.ValueZat,
						MemoHex:          n.MemoHex,
						Status: types.TxStatus{
							State:         types.TxStateConfirmed,
							Height:        blk.Height,
							Confirmations: 1,
						},
					},
					RecipientAddress: n.RecipientAddress,
					NoteNullifier:    n.NoteNullifier,
				}
				payloadBytes, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("scanner: marshal event payload: %w", err)
				}
				if err := tx.InsertEvent(ctx, store.Event{
					Kind:     "DepositEvent",
					WalletID: n.WalletID,
					Height:   blk.Height,
					Payload:  payloadBytes,
				}); err != nil {
					return err
				}
			}
		}

		if err := s.confirmDepositConfirmations(ctx, tx, blk.Height); err != nil {
			return err
		}
		if err := s.confirmSpendConfirmations(ctx, tx, blk.Height); err != nil {
			return err
		}

		return nil
	})
}

func (s *Scanner) confirmDepositConfirmations(ctx context.Context, tx store.Tx, scanHeight int64) error {
	maxNoteHeight := scanHeight - s.confirmations + 1
	if maxNoteHeight < 0 {
		return nil
	}

	notes, err := tx.ConfirmNotes(ctx, scanHeight, maxNoteHeight)
	if err != nil {
		return err
	}

	for _, n := range notes {
		memoHex := ""
		if n.MemoHex != nil {
			memoHex = *n.MemoHex
		}
		confirmations := scanHeight - n.Height + 1
		if confirmations < 1 {
			confirmations = 1
		}

		payload := depositConfirmedPayload{
			depositEventPayload: depositEventPayload{
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
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     "DepositConfirmed",
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scanner) confirmSpendConfirmations(ctx context.Context, tx store.Tx, scanHeight int64) error {
	maxSpentHeight := scanHeight - s.confirmations + 1
	if maxSpentHeight < 0 {
		return nil
	}

	notes, err := tx.ConfirmSpends(ctx, scanHeight, maxSpentHeight)
	if err != nil {
		return err
	}

	for _, n := range notes {
		if n.SpentHeight == nil || n.SpentTxID == nil {
			continue
		}
		confirmations := scanHeight - *n.SpentHeight + 1
		if confirmations < 1 {
			confirmations = 1
		}

		payload := spendConfirmedPayload{
			spendEventPayload: spendEventPayload{
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
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal spend confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     "SpendConfirmed",
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}
	return nil
}

type depositEventPayload struct {
	types.DepositEvent
	RecipientAddress string `json:"recipient_address,omitempty"`
	NoteNullifier    string `json:"note_nullifier,omitempty"`
}

type depositConfirmedPayload struct {
	depositEventPayload
	ConfirmedHeight       int64 `json:"confirmed_height"`
	RequiredConfirmations int64 `json:"required_confirmations"`
}

type spendEventPayload struct {
	Version          types.Version `json:"version"`
	WalletID         string        `json:"wallet_id"`
	DiversifierIndex uint32        `json:"diversifier_index,omitempty"`
	TxID             string        `json:"txid"`
	Height           int64         `json:"height"`

	NoteTxID        string `json:"note_txid"`
	NoteActionIndex uint32 `json:"note_action_index"`
	NoteHeight      int64  `json:"note_height"`
	AmountZatoshis  uint64 `json:"amount_zatoshis"`
	NoteNullifier   string `json:"note_nullifier,omitempty"`

	RecipientAddress string         `json:"recipient_address,omitempty"`
	Status           types.TxStatus `json:"status"`
}

type spendConfirmedPayload struct {
	spendEventPayload
	ConfirmedHeight       int64 `json:"confirmed_height"`
	RequiredConfirmations int64 `json:"required_confirmations"`
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
