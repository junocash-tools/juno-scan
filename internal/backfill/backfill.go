package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

	InsertedNotes  int64
	InsertedEvents int64
}

func (s *Service) BackfillWallet(ctx context.Context, req Request) (Result, error) {
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

	walletUFVK, ok, err := s.walletUFVK(ctx, req.WalletID)
	if err != nil {
		return Result{}, err
	}
	if !ok {
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

	for height := req.FromHeight; height <= scanTo; height++ {
		hash, ok, err := s.st.HashAtHeight(ctx, height)
		if err != nil {
			return Result{}, err
		}
		if !ok {
			return Result{}, fmt.Errorf("backfill: missing block at height %d (scanner tip is behind)", height)
		}

		var blk blockVerbose2
		if err := s.rpc.Call(ctx, "getblock", []any{hash, 2}, &blk); err != nil {
			return Result{}, fmt.Errorf("backfill: getblock(%d): %w", height, err)
		}
		if blk.Height != height {
			return Result{}, fmt.Errorf("backfill: daemon returned unexpected height: got %d want %d", blk.Height, height)
		}

		startPos := nextPos

		err = s.st.WithTx(ctx, func(tx store.Tx) error {
			posByTxAction := make(map[string]map[uint32]int64)

			for _, t := range blk.Tx {
				res, err := orchardscan.ScanTx(ctx, s.uaHRP, []orchardscan.Wallet{{WalletID: req.WalletID, UFVK: walletUFVK}}, t.Hex)
				if err != nil {
					return fmt.Errorf("backfill: scan tx %s: %w", t.TxID, err)
				}

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

				// Record outgoing outputs for spends (recipient/value/memo via OVK output recovery).
				if len(spentNotes) > 0 {
					outs, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, []orchardscan.Wallet{{WalletID: req.WalletID, UFVK: walletUFVK}}, t.Hex)
					if err != nil {
						return fmt.Errorf("backfill: recover outgoing tx %s: %w", t.TxID, err)
					}

					for _, o := range outs {
						heightCopy := height

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

							TxID:  t.TxID,
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

						payload := events.DepositEventPayload{
							DepositEvent: types.DepositEvent{
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

	nextHeight := scanTo + 1
	if nextHeight < req.FromHeight {
		nextHeight = req.FromHeight
	}

	return Result{
		FromHeight:     req.FromHeight,
		ToHeight:       req.ToHeight,
		ScannedFrom:    req.FromHeight,
		ScannedTo:      scanTo,
		NextHeight:     nextHeight,
		InsertedNotes:  insertedNotes,
		InsertedEvents: insertedEvents,
	}, nil
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

				TxID:  o.TxID,
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
