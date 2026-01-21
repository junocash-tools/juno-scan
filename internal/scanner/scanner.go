package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/zmq"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/Abdullah1738/juno-sdk-go/types"
)

type Scanner struct {
	st    store.Store
	rpc   *sdkjunocashd.Client
	uaHRP string

	pollInterval  time.Duration
	confirmations int64

	zmqHashBlockEndpoint string

	mempoolOrchardNullifiersByTxID map[string][]string
}

func New(st store.Store, rpc *sdkjunocashd.Client, uaHRP string, pollInterval time.Duration, confirmations int64, zmqHashBlockEndpoint string) (*Scanner, error) {
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
	if zmqHashBlockEndpoint != "" {
		if _, err := zmq.ParseEndpoint(zmqHashBlockEndpoint); err != nil {
			return nil, fmt.Errorf("scanner: zmq-hashblock: %w", err)
		}
	}
	return &Scanner{
		st:                   st,
		rpc:                  rpc,
		uaHRP:                uaHRP,
		pollInterval:         pollInterval,
		confirmations:        confirmations,
		zmqHashBlockEndpoint: zmqHashBlockEndpoint,
	}, nil
}

func (s *Scanner) Run(ctx context.Context) error {
	var zmqNotify <-chan struct{}
	if s.zmqHashBlockEndpoint != "" {
		ch := make(chan struct{}, 1)
		zmqNotify = ch
		go func() {
			_ = zmq.Notify(ctx, zmq.NotifyConfig{
				Endpoint: s.zmqHashBlockEndpoint,
				Topic:    "hashblock",
			}, ch, log.Printf)
		}()
	}

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
		case <-zmqNotify:
		}
	}
}

func (s *Scanner) scanOnce(ctx context.Context) error {
	for ctx.Err() == nil {
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
			if err := s.updatePendingSpends(ctx); err != nil {
				return err
			}
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
				continue
			}
		}

		var blk blockVerbose2
		if err := s.rpc.Call(ctx, "getblock", []any{nextHash, 2}, &blk); err != nil {
			return fmt.Errorf("scanner: getblock(%d): %w", nextHeight, err)
		}
		if blk.Height != nextHeight {
			return fmt.Errorf("scanner: daemon returned unexpected height: got %d want %d", blk.Height, nextHeight)
		}

		if err := s.processBlock(ctx, blk); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (s *Scanner) updatePendingSpends(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var txids []string
	if err := s.rpc.Call(ctx, "getrawmempool", nil, &txids); err != nil {
		return fmt.Errorf("scanner: getrawmempool: %w", err)
	}

	type rawTx struct {
		Orchard struct {
			Actions []struct {
				Nullifier string `json:"nullifier"`
			} `json:"actions"`
		} `json:"orchard"`
	}

	if s.mempoolOrchardNullifiersByTxID == nil {
		s.mempoolOrchardNullifiersByTxID = make(map[string][]string)
	}

	inMempool := make(map[string]struct{}, len(txids))
	for _, txid := range txids {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			continue
		}
		inMempool[txid] = struct{}{}
	}

	for txid := range s.mempoolOrchardNullifiersByTxID {
		if _, ok := inMempool[txid]; !ok {
			delete(s.mempoolOrchardNullifiersByTxID, txid)
		}
	}

	for txid := range inMempool {
		if _, ok := s.mempoolOrchardNullifiersByTxID[txid]; ok {
			continue
		}

		var tx rawTx
		if err := s.rpc.Call(ctx, "getrawtransaction", []any{txid, 1}, &tx); err != nil {
			var rpcErr *sdkjunocashd.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == -5 {
				continue
			}
			log.Printf("mempool: getrawtransaction(%s) failed: %v", txid, err)
			continue
		}

		nfs := make([]string, 0, len(tx.Orchard.Actions))
		for _, a := range tx.Orchard.Actions {
			nf := strings.ToLower(strings.TrimSpace(a.Nullifier))
			if nf == "" {
				continue
			}
			nfs = append(nfs, nf)
		}

		s.mempoolOrchardNullifiersByTxID[txid] = nfs
	}

	pending := make(map[string]string)
	for txid, nfs := range s.mempoolOrchardNullifiersByTxID {
		for _, nf := range nfs {
			if nf == "" {
				continue
			}
			pending[nf] = txid
		}
	}

	if err := s.st.UpdatePendingSpends(ctx, pending, time.Now().UTC()); err != nil {
		return fmt.Errorf("scanner: update pending spends: %w", err)
	}
	return nil
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
				payload := events.SpendEventPayload{
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
					Kind:     events.KindSpendEvent,
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
				inserted, err := tx.InsertNote(ctx, store.Note{
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
				})
				if err != nil {
					return err
				}

				if inserted {
					payload := events.DepositEventPayload{
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
						Kind:     events.KindDepositEvent,
						WalletID: n.WalletID,
						Height:   blk.Height,
						Payload:  payloadBytes,
					}); err != nil {
						return err
					}
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

		payload := events.DepositConfirmedPayload{
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
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindDepositConfirmed,
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

		payload := events.SpendConfirmedPayload{
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
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal spend confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindSpendConfirmed,
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}
	return nil
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
