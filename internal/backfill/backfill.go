package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/Abdullah1738/juno-sdk-go/types"
)

type Service struct {
	st            store.Store
	rpc           *sdkjunocashd.Client
	uaHRP         string
	confirmations int64
	mu            sync.RWMutex
	active        int
}

type Status struct {
	Active     int `json:"active"`
	QueueDepth int `json:"queue_depth"`
}

func (s *Service) Snapshot() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Status{Active: s.active}
}

type orchardActionIndexStore interface {
	ListOrchardActionHeights(context.Context, int64, int64) ([]int64, error)
}

func New(st store.Store, rpc *sdkjunocashd.Client, uaHRP string, confirmations int64) (*Service, error) {
	if st == nil {
		return nil, errors.New("backfill: store is nil")
	}
	if rpc == nil {
		return nil, errors.New("backfill: rpc is nil")
	}
	if strings.TrimSpace(uaHRP) == "" {
		return nil, errors.New("backfill: ua hrp is required")
	}
	if confirmations <= 0 {
		confirmations = 100
	}
	return &Service{st: st, rpc: rpc, uaHRP: uaHRP, confirmations: confirmations}, nil
}

type Request struct {
	WalletID   string
	FromHeight int64
	ToHeight   int64
	BatchSize  int64
}

type Result struct {
	FromHeight int64
	ToHeight   int64

	ScannedFrom int64
	ScannedTo   int64
	NextHeight  int64

	InsertedNotes        int64
	InsertedEvents       int64
	VisitedActionHeights int64
	SkippedHeights       int64
	RPCCalls             int64
}

func (s *Service) BackfillWallet(ctx context.Context, req Request) (Result, error) {
	s.mu.Lock()
	s.active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()
	req.WalletID = strings.TrimSpace(req.WalletID)
	if req.WalletID == "" {
		return Result{}, errors.New("backfill: wallet_id required")
	}
	if req.FromHeight < 0 {
		return Result{}, errors.New("backfill: from_height must be >= 0")
	}
	if req.ToHeight < req.FromHeight {
		return Result{}, errors.New("backfill: to_height must be >= from_height")
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 100
	}
	if req.BatchSize > 10_000 {
		req.BatchSize = 10_000
	}

	registered, err := s.st.ListEnabledWalletUFVKs(ctx)
	if err != nil {
		return Result{}, err
	}
	wallets := make([]orchardscan.Wallet, 0, len(registered))
	walletFound := false
	for _, w := range registered {
		wallets = append(wallets, orchardscan.Wallet{WalletID: w.WalletID, UFVK: w.UFVK})
		if w.WalletID == req.WalletID {
			walletFound = true
		}
	}
	if !walletFound {
		return Result{}, errors.New("backfill: wallet not found (or disabled)")
	}

	scanTo := req.ToHeight
	if req.FromHeight+req.BatchSize-1 < scanTo {
		scanTo = req.FromHeight + req.BatchSize - 1
	}

	nextPos := int64(0)
	if pos, ok, err := s.st.FirstOrchardCommitmentPositionFromHeight(ctx, req.FromHeight); err != nil {
		return Result{}, err
	} else if ok {
		nextPos = pos
	}

	var insertedNotes int64
	var insertedEvents int64
	actionHeights := make([]int64, 0, scanTo-req.FromHeight+1)
	if indexed, ok := s.st.(orchardActionIndexStore); ok {
		actionHeights, err = indexed.ListOrchardActionHeights(ctx, req.FromHeight, scanTo)
		if err != nil {
			return Result{}, fmt.Errorf("backfill: action index: %w", err)
		}
	} else {
		for height := req.FromHeight; height <= scanTo; height++ {
			actionHeights = append(actionHeights, height)
		}
	}
	var rpcCalls int64

	for _, height := range actionHeights {
		hash, calls, err := s.canonicalHashAtHeight(ctx, height)
		rpcCalls += calls
		if err != nil {
			return Result{}, err
		}

		var blk blockVerbose2
		if err := s.rpc.Call(ctx, "getblock", []any{hash, 2}, &blk); err != nil {
			return Result{}, fmt.Errorf("backfill: getblock(%d): %w", height, err)
		}
		rpcCalls++
		if blk.Height != height || strings.ToLower(strings.TrimSpace(blk.Hash)) != hash {
			return Result{}, fmt.Errorf("%w: daemon block mismatch at height %d", store.ErrCanonicalBlockChanged, height)
		}

		startPos := nextPos

		err = s.st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.AssertCanonicalBlock(ctx, height, hash); err != nil {
				return fmt.Errorf("backfill: canonical fence height %d: %w", height, err)
			}
			posByTxAction := make(map[string]map[uint32]int64)
			txHexes := make([]string, 0, len(blk.Tx))
			for _, t := range blk.Tx {
				txHexes = append(txHexes, t.Hex)
			}
			scanResults, err := orchardscan.ScanBlock(ctx, s.uaHRP, wallets, txHexes)
			if err != nil {
				return fmt.Errorf("backfill: scan block %d: %w", height, err)
			}

			for txIndex, t := range blk.Tx {
				res := scanResults[txIndex]

				if len(res.Actions) == 0 {
					continue
				}
				if _, ok := posByTxAction[t.TxID]; !ok {
					posByTxAction[t.TxID] = make(map[uint32]int64, len(res.Actions))
				}
				for _, a := range res.Actions {
					posByTxAction[t.TxID][a.ActionIndex] = nextPos
					nextPos++
				}

				nullifiers := make([]string, 0, len(res.Actions))
				for _, a := range res.Actions {
					nullifiers = append(nullifiers, a.ActionNullifier)
				}

				spentNotes, err := tx.MarkNotesSpent(ctx, height, t.TxID, nullifiers)
				if err != nil {
					return err
				}
				for _, n := range spentNotes {
					payload := events.SpendEventPayload{
						Version:          types.V1,
						WalletID:         n.WalletID,
						DiversifierIndex: n.DiversifierIndex,
						TxID:             t.TxID,
						Height:           height,
						NoteTxID:         n.TxID,
						NoteActionIndex:  uint32(n.ActionIndex),
						NoteHeight:       n.Height,
						AmountZatoshis:   uint64(n.ValueZat),
						NoteNullifier:    n.NoteNullifier,
						RecipientAddress: n.RecipientAddress,
						Status: types.TxStatus{
							State:         types.TxStateConfirmed,
							Height:        height,
							Confirmations: 1,
						},
					}
					b, err := json.Marshal(payload)
					if err != nil {
						return fmt.Errorf("backfill: marshal spend payload: %w", err)
					}
					if err := tx.InsertEvent(ctx, store.Event{
						Kind:     events.KindSpendEvent,
						WalletID: n.WalletID,
						Height:   height,
						Payload:  b,
					}); err != nil {
						return err
					}
					insertedEvents++
				}

				recoveredByRegisteredWallet, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, wallets, t.Hex)
				if err != nil {
					return fmt.Errorf("backfill: classify transaction origin %s: %w", t.TxID, err)
				}
				isInternalTx := len(spentNotes) > 0 || len(recoveredByRegisteredWallet) > 0
				if isInternalTx {
					if _, err := tx.MarkTransactionInternal(ctx, t.TxID); err != nil {
						return err
					}
				}

				// Record outgoing outputs for spends (recipient/value/memo via OVK output recovery).
				if len(spentNotes) > 0 {
					spentWalletIDs := make(map[string]struct{}, len(spentNotes))
					for _, n := range spentNotes {
						spentWalletIDs[n.WalletID] = struct{}{}
					}
					recoveryWallets := make([]orchardscan.Wallet, 0, len(spentWalletIDs))
					for _, wallet := range wallets {
						if _, spent := spentWalletIDs[wallet.WalletID]; spent {
							recoveryWallets = append(recoveryWallets, wallet)
						}
					}
					if len(recoveryWallets) != len(spentWalletIDs) {
						return errors.New("backfill: spent-note wallet identity changed")
					}
					outs, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, recoveryWallets, t.Hex)
					if err != nil {
						return fmt.Errorf("backfill: recover outgoing tx %s: %w", t.TxID, err)
					}

					for _, o := range outs {
						heightCopy := height
						pos := posByTxAction[t.TxID][o.ActionIndex]
						posCopy := pos

						var memoHexPtr *string
						memoHex := strings.TrimSpace(o.MemoHex)
						if memoHex != "" {
							memoHexPtr = &memoHex
						}

						var recipientScopePtr *string
						recipientScope := strings.TrimSpace(o.RecipientScope)
						if recipientScope != "" {
							recipientScopePtr = &recipientScope
						}

						changed, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
							WalletID: o.WalletID,
							TxID:     t.TxID,

							ActionIndex: int32(o.ActionIndex),

							MinedHeight: &heightCopy,
							Position:    &posCopy,

							RecipientAddress: o.RecipientAddress,
							ValueZat:         int64(o.ValueZat),
							MemoHex:          memoHexPtr,

							OvkScope:       o.OvkScope,
							RecipientScope: recipientScopePtr,
						})
						if err != nil {
							return err
						}
						if !changed {
							continue
						}

						payload := events.OutgoingOutputEventPayload{
							Version:  types.V1,
							WalletID: o.WalletID,

							TxID:   t.TxID,
							Height: &heightCopy,

							ActionIndex:    o.ActionIndex,
							AmountZatoshis: o.ValueZat,

							RecipientAddress: o.RecipientAddress,
							MemoHex:          memoHex,

							OvkScope:       o.OvkScope,
							RecipientScope: recipientScope,

							Status: types.TxStatus{
								State:         types.TxStateConfirmed,
								Height:        height,
								Confirmations: 1,
							},
						}
						b, err := json.Marshal(payload)
						if err != nil {
							return fmt.Errorf("backfill: marshal outgoing output payload: %w", err)
						}
						if err := tx.InsertEvent(ctx, store.Event{
							Kind:     events.KindOutgoingOutputEvent,
							WalletID: o.WalletID,
							Height:   height,
							Payload:  b,
						}); err != nil {
							return err
						}
						insertedEvents++
					}
				}

				for _, n := range res.Notes {
					if n.WalletID != req.WalletID {
						continue
					}
					pos, ok := posByTxAction[t.TxID][n.ActionIndex]
					if !ok {
						continue
					}
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
						Height:           height,
						Position:         &posCopy,
						IsInternal:       isInternalTx,
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
						insertedNotes++
					}
					if inserted && !isInternalTx {
						payload := events.DepositEventPayload{
							Origin: string(types.DepositOriginExternal),
							DepositEvent: events.DepositEvent{
								Version:          types.V1,
								WalletID:         n.WalletID,
								DiversifierIndex: n.DiversifierIndex,
								TxID:             t.TxID,
								Height:           height,
								ActionIndex:      n.ActionIndex,
								AmountZatoshis:   n.ValueZat,
								MemoHex:          n.MemoHex,
								Status: types.TxStatus{
									State:         types.TxStateConfirmed,
									Height:        height,
									Confirmations: 1,
								},
							},
							RecipientAddress: n.RecipientAddress,
							NoteNullifier:    n.NoteNullifier,
						}
						b, err := json.Marshal(payload)
						if err != nil {
							return fmt.Errorf("backfill: marshal deposit payload: %w", err)
						}
						if err := tx.InsertEvent(ctx, store.Event{
							Kind:     events.KindDepositEvent,
							WalletID: n.WalletID,
							Height:   height,
							Payload:  b,
						}); err != nil {
							return err
						}
						insertedEvents++
					}
				}
			}

			confirmedNotes, confirmedNoteEvents, err := confirmDepositConfirmations(ctx, tx, s.confirmations, height)
			if err != nil {
				return err
			}
			insertedEvents += confirmedNoteEvents
			_ = confirmedNotes

			confirmedSpends, confirmedSpendEvents, err := confirmSpendConfirmations(ctx, tx, s.confirmations, height)
			if err != nil {
				return err
			}
			insertedEvents += confirmedSpendEvents
			_ = confirmedSpends

			confirmedOutgoing, confirmedOutgoingEvents, err := confirmOutgoingOutputConfirmations(ctx, tx, s.confirmations, height)
			if err != nil {
				return err
			}
			insertedEvents += confirmedOutgoingEvents
			_ = confirmedOutgoing

			return nil
		})
		if err != nil {
			nextPos = startPos
			return Result{}, err
		}
	}

	// Coalesce confirmation progress across barren ranges into one final write.
	finalHash, calls, err := s.canonicalHashAtHeight(ctx, scanTo)
	rpcCalls += calls
	if err != nil {
		return Result{}, err
	}
	if err := s.st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.AssertCanonicalBlock(ctx, scanTo, finalHash); err != nil {
			return fmt.Errorf("backfill: final canonical fence height %d: %w", scanTo, err)
		}
		_, eventsAdded, err := confirmDepositConfirmations(ctx, tx, s.confirmations, scanTo)
		if err != nil {
			return err
		}
		insertedEvents += eventsAdded
		_, eventsAdded, err = confirmSpendConfirmations(ctx, tx, s.confirmations, scanTo)
		if err != nil {
			return err
		}
		insertedEvents += eventsAdded
		_, eventsAdded, err = confirmOutgoingOutputConfirmations(ctx, tx, s.confirmations, scanTo)
		if err != nil {
			return err
		}
		insertedEvents += eventsAdded
		return nil
	}); err != nil {
		return Result{}, err
	}

	nextHeight := scanTo + 1
	if nextHeight < req.FromHeight {
		nextHeight = req.FromHeight
	}

	return Result{
		FromHeight:           req.FromHeight,
		ToHeight:             req.ToHeight,
		ScannedFrom:          req.FromHeight,
		ScannedTo:            scanTo,
		NextHeight:           nextHeight,
		InsertedNotes:        insertedNotes,
		InsertedEvents:       insertedEvents,
		VisitedActionHeights: int64(len(actionHeights)),
		SkippedHeights:       (scanTo - req.FromHeight + 1) - int64(len(actionHeights)),
		RPCCalls:             rpcCalls,
	}, nil
}

func (s *Service) canonicalHashAtHeight(ctx context.Context, height int64) (string, int64, error) {
	stored, ok, err := s.st.HashAtHeight(ctx, height)
	if err != nil {
		return "", 0, err
	}
	stored = strings.ToLower(strings.TrimSpace(stored))
	if !ok || stored == "" {
		return "", 0, fmt.Errorf("backfill: missing block at height %d (scanner tip is behind)", height)
	}
	var canonical string
	if err := s.rpc.Call(ctx, "getblockhash", []any{height}, &canonical); err != nil {
		return "", 1, fmt.Errorf("backfill: getblockhash(%d): %w", height, err)
	}
	canonical = strings.ToLower(strings.TrimSpace(canonical))
	if canonical == "" || canonical != stored {
		return "", 1, fmt.Errorf("%w: height %d stored=%s rpc=%s", store.ErrCanonicalBlockChanged, height, stored, canonical)
	}
	return stored, 1, nil
}

func (s *Service) walletUFVK(ctx context.Context, walletID string) (string, bool, error) {
	ws, err := s.st.ListEnabledWalletUFVKs(ctx)
	if err != nil {
		return "", false, err
	}
	for _, w := range ws {
		if strings.TrimSpace(w.WalletID) == walletID {
			return strings.TrimSpace(w.UFVK), true, nil
		}
	}
	return "", false, nil
}

func confirmDepositConfirmations(ctx context.Context, tx store.Tx, confirmations int64, scanHeight int64) ([]store.Note, int64, error) {
	maxNoteHeight := scanHeight - confirmations + 1
	if maxNoteHeight < 0 {
		return nil, 0, nil
	}

	notes, err := tx.ConfirmNotes(ctx, scanHeight, maxNoteHeight)
	if err != nil {
		return nil, 0, err
	}

	var inserted int64
	for _, n := range notes {
		if n.IsInternal {
			continue
		}
		memoHex := ""
		if n.MemoHex != nil {
			memoHex = *n.MemoHex
		}
		confCount := scanHeight - n.Height + 1
		if confCount < 1 {
			confCount = 1
		}

		payload := events.DepositConfirmedPayload{
			DepositEventPayload: events.DepositEventPayload{
				Origin: string(types.DepositOriginExternal),
				DepositEvent: events.DepositEvent{
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
						Confirmations: confCount,
					},
				},
				RecipientAddress: n.RecipientAddress,
				NoteNullifier:    n.NoteNullifier,
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: confirmations,
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("backfill: marshal deposit confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindDepositConfirmed,
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  b,
		}); err != nil {
			return nil, 0, err
		}
		inserted++
	}
	return notes, inserted, nil
}

func confirmSpendConfirmations(ctx context.Context, tx store.Tx, confirmations int64, scanHeight int64) ([]store.Note, int64, error) {
	maxSpentHeight := scanHeight - confirmations + 1
	if maxSpentHeight < 0 {
		return nil, 0, nil
	}

	notes, err := tx.ConfirmSpends(ctx, scanHeight, maxSpentHeight)
	if err != nil {
		return nil, 0, err
	}

	var inserted int64
	for _, n := range notes {
		if n.SpentHeight == nil || n.SpentTxID == nil {
			continue
		}
		confCount := scanHeight - *n.SpentHeight + 1
		if confCount < 1 {
			confCount = 1
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
					Confirmations: confCount,
				},
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: confirmations,
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("backfill: marshal spend confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindSpendConfirmed,
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  b,
		}); err != nil {
			return nil, 0, err
		}
		inserted++
	}
	return notes, inserted, nil
}

func confirmOutgoingOutputConfirmations(ctx context.Context, tx store.Tx, confirmations int64, scanHeight int64) ([]store.OutgoingOutput, int64, error) {
	maxMinedHeight := scanHeight - confirmations + 1
	if maxMinedHeight < 0 {
		return nil, 0, nil
	}

	outs, err := tx.ConfirmOutgoingOutputs(ctx, scanHeight, maxMinedHeight)
	if err != nil {
		return nil, 0, err
	}

	var inserted int64
	for _, o := range outs {
		if o.MinedHeight == nil {
			continue
		}
		confCount := scanHeight - *o.MinedHeight + 1
		if confCount < 1 {
			confCount = 1
		}

		memoHex := ""
		if o.MemoHex != nil {
			memoHex = *o.MemoHex
		}

		heightCopy := *o.MinedHeight
		payload := events.OutgoingOutputConfirmedPayload{
			OutgoingOutputEventPayload: events.OutgoingOutputEventPayload{
				Version:  types.V1,
				WalletID: o.WalletID,

				TxID:   o.TxID,
				Height: &heightCopy,

				ActionIndex:    uint32(o.ActionIndex),
				AmountZatoshis: uint64(o.ValueZat),

				RecipientAddress: o.RecipientAddress,
				MemoHex:          memoHex,

				OvkScope:       o.OvkScope,
				RecipientScope: derefString(o.RecipientScope),

				Status: types.TxStatus{
					State:         types.TxStateConfirmed,
					Height:        *o.MinedHeight,
					Confirmations: confCount,
				},
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: confirmations,
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("backfill: marshal outgoing output confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindOutgoingOutputConfirmed,
			WalletID: o.WalletID,
			Height:   scanHeight,
			Payload:  b,
		}); err != nil {
			return nil, 0, err
		}
		inserted++
	}
	return outs, inserted, nil
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

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
