package rocksdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-sdk-go/types"
	"github.com/cockroachdb/pebble"
)

type Store struct {
	mu sync.Mutex
	db *pebble.DB
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("rocksdb: path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("rocksdb: mkdir: %w", err)
	}

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: open: %w", err)
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
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	verKey := keyMeta("schema_version")
	_, closer, err := s.db.Get(verKey)
	if err == nil {
		_ = closer.Close()
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: schema_version: %w", err)
	}

	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(verKey, []byte("1"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set schema_version: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate commit: %w", err)
	}
	return nil
}

func (s *Store) WithTx(ctx context.Context, fn func(store.Tx) error) error {
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	tx := &rocksTx{
		db:    s.db,
		batch: batch,
		now:   time.Now().UTC(),
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: commit: %w", err)
	}
	return nil
}

func (s *Store) UpsertWallet(ctx context.Context, walletID, ufvk string) error {
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	key := keyWallet(walletID)
	now := time.Now().UTC()

	var rec walletRecord
	v, closer, err := s.db.Get(key)
	if err == nil {
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: decode wallet: %w", err)
		}
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get wallet: %w", err)
	} else {
		rec.CreatedAtUnix = now.Unix()
	}

	rec.UFVK = ufvk
	rec.DisabledAtUnix = nil

	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode wallet: %w", err)
	}

	batch := s.db.NewIndexedBatch()
	defer batch.Close()
	if err := batch.Set(key, b, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: upsert wallet: %w", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: upsert wallet commit: %w", err)
	}
	return nil
}

func (s *Store) ListWallets(ctx context.Context) ([]store.Wallet, error) {
	_ = ctx

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: walletPrefix,
		UpperBound: prefixUpperBound(walletPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.Wallet
	for iter.First(); iter.Valid(); iter.Next() {
		walletID := string(bytes.TrimPrefix(iter.Key(), walletPrefix))
		var rec walletRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode wallet: %w", err)
		}
		w := store.Wallet{
			WalletID:  walletID,
			CreatedAt: time.Unix(rec.CreatedAtUnix, 0).UTC(),
		}
		if rec.DisabledAtUnix != nil {
			t := time.Unix(*rec.DisabledAtUnix, 0).UTC()
			w.DisabledAt = &t
		}
		out = append(out, w)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list wallets: %w", err)
	}
	return out, nil
}

func (s *Store) ListEnabledWalletUFVKs(ctx context.Context) ([]store.WalletUFVK, error) {
	_ = ctx

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: walletPrefix,
		UpperBound: prefixUpperBound(walletPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.WalletUFVK
	for iter.First(); iter.Valid(); iter.Next() {
		walletID := string(bytes.TrimPrefix(iter.Key(), walletPrefix))
		var rec walletRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode wallet: %w", err)
		}
		if rec.DisabledAtUnix != nil {
			continue
		}
		out = append(out, store.WalletUFVK{
			WalletID: walletID,
			UFVK:     rec.UFVK,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list enabled wallets: %w", err)
	}
	return out, nil
}

func (s *Store) Tip(ctx context.Context) (store.BlockTip, bool, error) {
	_ = ctx

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: blockPrefix,
		UpperBound: prefixUpperBound(blockPrefix),
	})
	if err != nil {
		return store.BlockTip{}, false, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return store.BlockTip{}, false, fmt.Errorf("rocksdb: tip: %w", err)
		}
		return store.BlockTip{}, false, nil
	}
	height, err := parseFixed20Int64(bytes.TrimPrefix(iter.Key(), blockPrefix))
	if err != nil {
		return store.BlockTip{}, false, fmt.Errorf("rocksdb: tip key: %w", err)
	}
	var rec blockRecord
	if err := json.Unmarshal(iter.Value(), &rec); err != nil {
		return store.BlockTip{}, false, fmt.Errorf("rocksdb: tip decode: %w", err)
	}
	return store.BlockTip{Height: height, Hash: rec.Hash}, true, nil
}

func (s *Store) HashAtHeight(ctx context.Context, height int64) (string, bool, error) {
	_ = ctx
	if height < 0 {
		return "", false, nil
	}

	v, closer, err := s.db.Get(keyBlock(height))
	if err == nil {
		var rec blockRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return "", false, fmt.Errorf("rocksdb: decode block: %w", err)
		}
		_ = closer.Close()
		return rec.Hash, true, nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return "", false, nil
	}
	return "", false, fmt.Errorf("rocksdb: get block: %w", err)
}

func (s *Store) RollbackToHeight(ctx context.Context, height int64) error {
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	if height < 0 {
		b := s.db.NewBatch()
		defer b.Close()
		if err := b.DeleteRange([]byte{0x00}, []byte{0xFF}, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: rollback clear: %w", err)
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: rollback commit: %w", err)
		}
		return nil
	}

	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	now := time.Now().UTC()
	rtx := &rocksTx{
		db:    s.db,
		batch: batch,
		now:   now,
	}

	var orphanDeposits []store.Note
	var unconfirmedDeposits []store.Note
	var orphanSpends []store.Note
	var unconfirmedSpends []store.Note
	var orphanOutgoingOutputs []store.OutgoingOutput
	var unconfirmedOutgoingOutputs []store.OutgoingOutput

	noteFromRecord := func(rec noteRecord) store.Note {
		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		return store.Note{
			WalletID:             rec.WalletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      rec.ConfirmedHeight,
			SpentConfirmedHeight: rec.SpentConfirmedHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		}
	}

	outgoingFromRecord := func(rec outgoingOutputRecord) store.OutgoingOutput {
		var mempoolSeenAt *time.Time
		if rec.MempoolSeenUnix != nil {
			t := time.Unix(*rec.MempoolSeenUnix, 0).UTC()
			mempoolSeenAt = &t
		}
		return store.OutgoingOutput{
			WalletID:         rec.WalletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      rec.MinedHeight,
			ConfirmedHeight:  rec.ConfirmedHeight,
			MempoolSeenAt:    mempoolSeenAt,
			RecipientAddress: rec.RecipientAddress,
			ValueZat:         rec.ValueZat,
			MemoHex:          rec.MemoHex,
			OvkScope:         rec.OvkScope,
			RecipientScope:   rec.RecipientScope,
			CreatedAt:        time.Unix(rec.CreatedAtUnix, 0).UTC(),
		}
	}

	if height >= 0 {
		// Notes created above height.
		{
			lower := make([]byte, 0, len(noteHeightPrefix)+20)
			lower = append(lower, noteHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(noteHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter notes: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				walletID, _, txid, actionIndex, err := parseNoteHeightIndexKey(iter.Key())
				if err != nil {
					_ = iter.Close()
					return err
				}
				noteKey := keyNote(walletID, txid, actionIndex)
				v, closer, err := s.db.Get(noteKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get note: %w", err)
				}
				var rec noteRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode note: %w", err)
				}
				_ = closer.Close()
				orphanDeposits = append(orphanDeposits, noteFromRecord(rec))
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter notes: %w", err)
			}
			_ = iter.Close()
		}

		// Notes that were previously deposit-confirmed above height.
		{
			lower := make([]byte, 0, len(noteConfirmedHeightPrefix)+20)
			lower = append(lower, noteConfirmedHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(noteConfirmedHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter confirmed notes: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				parts := bytes.Split(iter.Key(), []byte("/"))
				if len(parts) != 3 {
					continue
				}
				nullifier := string(parts[2])
				locBytes, closer, err := s.db.Get(keyNullifier(nullifier))
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get nullifier: %w", err)
				}
				noteKey := append([]byte{}, locBytes...)
				_ = closer.Close()

				v, closer, err := s.db.Get(noteKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get note: %w", err)
				}
				var rec noteRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode note: %w", err)
				}
				_ = closer.Close()

				if rec.Height <= height && rec.ConfirmedHeight != nil && *rec.ConfirmedHeight > height {
					unconfirmedDeposits = append(unconfirmedDeposits, noteFromRecord(rec))
				}
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter confirmed notes: %w", err)
			}
			_ = iter.Close()
		}

		// Notes that were spent above height.
		{
			lower := make([]byte, 0, len(noteSpentHeightPrefix)+20)
			lower = append(lower, noteSpentHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(noteSpentHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter spent notes: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				parts := bytes.Split(iter.Key(), []byte("/"))
				if len(parts) != 3 {
					continue
				}
				nullifier := string(parts[2])
				locBytes, closer, err := s.db.Get(keyNullifier(nullifier))
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get nullifier: %w", err)
				}
				noteKey := append([]byte{}, locBytes...)
				_ = closer.Close()

				v, closer, err := s.db.Get(noteKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get note: %w", err)
				}
				var rec noteRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode note: %w", err)
				}
				_ = closer.Close()

				if rec.Height <= height && rec.SpentHeight != nil && *rec.SpentHeight > height {
					orphanSpends = append(orphanSpends, noteFromRecord(rec))
				}
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter spent notes: %w", err)
			}
			_ = iter.Close()
		}

		// Spends that were confirmed above height.
		{
			lower := make([]byte, 0, len(noteSpentConfirmedHeightPrefix)+20)
			lower = append(lower, noteSpentConfirmedHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(noteSpentConfirmedHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter confirmed spends: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				parts := bytes.Split(iter.Key(), []byte("/"))
				if len(parts) != 3 {
					continue
				}
				nullifier := string(parts[2])
				locBytes, closer, err := s.db.Get(keyNullifier(nullifier))
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get nullifier: %w", err)
				}
				noteKey := append([]byte{}, locBytes...)
				_ = closer.Close()

				v, closer, err := s.db.Get(noteKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get note: %w", err)
				}
				var rec noteRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode note: %w", err)
				}
				_ = closer.Close()

				if rec.Height <= height &&
					rec.SpentHeight != nil && *rec.SpentHeight <= height &&
					rec.SpentConfirmedHeight != nil && *rec.SpentConfirmedHeight > height {
					unconfirmedSpends = append(unconfirmedSpends, noteFromRecord(rec))
				}
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter confirmed spends: %w", err)
			}
			_ = iter.Close()
		}

		// Outgoing outputs mined above height.
		{
			lower := make([]byte, 0, len(outgoingMinedHeightPrefix)+20)
			lower = append(lower, outgoingMinedHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(outgoingMinedHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter outgoing outputs: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				walletID, _, txid, actionIndex, err := parseOutgoingMinedHeightIndexKey(iter.Key())
				if err != nil {
					_ = iter.Close()
					return err
				}
				ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
				v, closer, err := s.db.Get(ooKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get outgoing output: %w", err)
				}
				var rec outgoingOutputRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode outgoing output: %w", err)
				}
				_ = closer.Close()
				orphanOutgoingOutputs = append(orphanOutgoingOutputs, outgoingFromRecord(rec))
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter outgoing outputs: %w", err)
			}
			_ = iter.Close()
		}

		// Outgoing outputs that were previously confirmed above height.
		{
			lower := make([]byte, 0, len(outgoingConfirmedHeightPrefix)+20)
			lower = append(lower, outgoingConfirmedHeightPrefix...)
			lower = appendUint64Fixed20(lower, uint64(height+1))

			iter, err := s.db.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: prefixUpperBound(outgoingConfirmedHeightPrefix),
			})
			if err != nil {
				return fmt.Errorf("rocksdb: rollback iter confirmed outgoing outputs: %w", err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				walletID, _, txid, actionIndex, err := parseOutgoingConfirmedHeightIndexKey(iter.Key())
				if err != nil {
					_ = iter.Close()
					return err
				}
				ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
				v, closer, err := s.db.Get(ooKey)
				if err != nil {
					if errors.Is(err, pebble.ErrNotFound) {
						continue
					}
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback get outgoing output: %w", err)
				}
				var rec outgoingOutputRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: rollback decode outgoing output: %w", err)
				}
				_ = closer.Close()

				if rec.MinedHeight != nil && *rec.MinedHeight <= height &&
					rec.ConfirmedHeight != nil && *rec.ConfirmedHeight > height {
					unconfirmedOutgoingOutputs = append(unconfirmedOutgoingOutputs, outgoingFromRecord(rec))
				}
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: rollback iter confirmed outgoing outputs: %w", err)
			}
			_ = iter.Close()
		}
	}

	// Delete blocks above height.
	if err := deleteRangeByFixed20(batch, blockPrefix, uint64(height+1)); err != nil {
		return err
	}

	// Delete orchard actions above height (via height index).
	if err := deleteIndexedByHeight(batch, s.db, orchardActionHeightPrefix, uint64(height+1), func(k []byte) [][]byte {
		// k = oah/<height>/<txid>/<action_index>
		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 4 {
			return nil
		}
		txid := parts[2]
		actionIdx := parts[3]
		oa := make([]byte, 0, len(orchardActionPrefix)+len(txid)+1+len(actionIdx))
		oa = append(oa, orchardActionPrefix...)
		oa = append(oa, txid...)
		oa = append(oa, '/')
		oa = append(oa, actionIdx...)
		return [][]byte{oa}
	}); err != nil {
		return err
	}

	// Delete events above height (via height index).
	if err := deleteIndexedByHeight(batch, s.db, eventHeightPrefix, uint64(height+1), func(k []byte) [][]byte {
		// k = eh/<height>/<wallet>/<id>
		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 4 {
			return nil
		}
		walletID := parts[2]
		id := parts[3]
		ev := make([]byte, 0, len(eventPrefix)+len(walletID)+1+len(id))
		ev = append(ev, eventPrefix...)
		ev = append(ev, walletID...)
		ev = append(ev, '/')
		ev = append(ev, id...)
		return [][]byte{ev}
	}); err != nil {
		return err
	}

	// Delete notes created above height (via height index).
	if err := deleteNotesAboveHeight(batch, s.db, uint64(height+1)); err != nil {
		return err
	}

	// Unspend notes spent above height (via spent height index).
	if err := unspendNotesAboveHeight(batch, s.db, uint64(height)); err != nil {
		return err
	}

	// Unconfirm notes confirmed above height (via confirmed height index).
	if err := unconfirmNotesAboveHeight(batch, s.db, uint64(height)); err != nil {
		return err
	}
	_ = batch.Delete(keyMeta("confirmed_upto_note_height"), pebble.NoSync)

	// Unconfirm spends confirmed above height.
	if err := unconfirmSpendsAboveHeight(batch, s.db, uint64(height)); err != nil {
		return err
	}
	_ = batch.Delete(keyMeta("confirmed_upto_spent_height"), pebble.NoSync)

	// Delete outgoing outputs mined above height (via mined height index).
	if err := deleteOutgoingOutputsAboveHeight(batch, s.db, uint64(height+1)); err != nil {
		return err
	}

	// Unconfirm outgoing outputs confirmed above height.
	if err := unconfirmOutgoingOutputsAboveHeight(batch, s.db, uint64(height)); err != nil {
		return err
	}
	_ = batch.Delete(keyMeta("confirmed_upto_outgoing_mined_height"), pebble.NoSync)

	// Delete commitments above height and fix next position.
	if err := rollbackCommitments(batch, s.db, uint64(height)); err != nil {
		return err
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
				return fmt.Errorf("rocksdb: rollback marshal deposit orphaned: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
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
				return fmt.Errorf("rocksdb: rollback marshal deposit unconfirmed: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
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
				return fmt.Errorf("rocksdb: rollback marshal spend orphaned: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
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
				return fmt.Errorf("rocksdb: rollback marshal spend unconfirmed: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
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
				return fmt.Errorf("rocksdb: rollback marshal outgoing output orphaned: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
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
				return fmt.Errorf("rocksdb: rollback marshal outgoing output unconfirmed: %w", err)
			}
			if err := rtx.InsertEvent(ctx, store.Event{
				Kind:     events.KindOutgoingOutputUnconfirmed,
				WalletID: o.WalletID,
				Height:   height,
				Payload:  b,
			}); err != nil {
				return err
			}
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: rollback commit: %w", err)
	}
	return nil
}

func (s *Store) WalletEventPublishCursor(ctx context.Context, walletID string) (int64, error) {
	_ = ctx

	s.mu.Lock()
	defer s.mu.Unlock()

	v, closer, err := s.db.Get(keyMeta("publish_cursor/" + walletID))
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return 0, errors.New("rocksdb: publish cursor corrupt")
		}
		n := binary.BigEndian.Uint64(v)
		_ = closer.Close()
		if n > uint64(^uint64(0)>>1) {
			return 0, errors.New("rocksdb: publish cursor overflow")
		}
		return int64(n), nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	return 0, fmt.Errorf("rocksdb: get publish cursor: %w", err)
}

func (s *Store) SetWalletEventPublishCursor(ctx context.Context, walletID string, cursor int64) error {
	_ = ctx
	if cursor < 0 {
		cursor = 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.db.NewBatch()
	defer b.Close()

	if err := b.Set(keyMeta("publish_cursor/"+walletID), uint64To8(uint64(cursor)), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set publish cursor: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set publish cursor commit: %w", err)
	}
	return nil
}

func (s *Store) ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int, filter store.EventFilter) ([]store.Event, int64, error) {
	_ = ctx
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if afterID < 0 {
		afterID = 0
	}
	if filter.BlockHeight != nil && *filter.BlockHeight < 0 {
		return nil, afterID, errors.New("rocksdb: negative blockHeight")
	}

	wantKind := make(map[string]struct{}, len(filter.Kinds))
	for _, k := range filter.Kinds {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		wantKind[k] = struct{}{}
	}
	wantTxID := strings.ToLower(strings.TrimSpace(filter.TxID))

	if filter.BlockHeight != nil {
		height := uint64(*filter.BlockHeight)

		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: keyEventHeightIndex(height, walletID, uint64(afterID+1)),
			UpperBound: prefixUpperBound(keyEventHeightPrefix(height, walletID)),
		})
		if err != nil {
			return nil, afterID, fmt.Errorf("rocksdb: iter: %w", err)
		}
		defer iter.Close()

		var out []store.Event
		nextCursor := afterID
		for iter.First(); iter.Valid(); iter.Next() {
			if len(out) >= limit {
				break
			}

			id, err := parseEventHeightIndexID(iter.Key(), height, walletID)
			if err != nil {
				return nil, afterID, err
			}

			v, closer, err := s.db.Get(keyEvent(walletID, id))
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					continue
				}
				return nil, afterID, fmt.Errorf("rocksdb: get event: %w", err)
			}

			var rec eventRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				_ = closer.Close()
				return nil, afterID, fmt.Errorf("rocksdb: decode event: %w", err)
			}
			_ = closer.Close()

			if len(wantKind) > 0 {
				if _, ok := wantKind[rec.Kind]; !ok {
					continue
				}
			}
			if wantTxID != "" {
				var p struct {
					TxID string `json:"txid"`
				}
				_ = json.Unmarshal([]byte(rec.Payload), &p)
				if strings.ToLower(strings.TrimSpace(p.TxID)) != wantTxID {
					continue
				}
			}

			nextCursor = int64(id)
			out = append(out, store.Event{
				ID:        int64(id),
				Kind:      rec.Kind,
				WalletID:  walletID,
				Height:    rec.Height,
				Payload:   json.RawMessage(rec.Payload),
				CreatedAt: time.Unix(rec.CreatedAtUnix, 0).UTC(),
			})
		}
		if err := iter.Error(); err != nil {
			return nil, afterID, fmt.Errorf("rocksdb: list events: %w", err)
		}
		return out, nextCursor, nil
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyEvent(walletID, uint64(afterID+1)),
		UpperBound: prefixUpperBound(keyEventPrefix(walletID)),
	})
	if err != nil {
		return nil, afterID, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.Event
	nextCursor := afterID
	for iter.First(); iter.Valid(); iter.Next() {
		if len(out) >= limit {
			break
		}
		id, err := parseEventID(iter.Key(), walletID)
		if err != nil {
			return nil, afterID, err
		}
		var rec eventRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, afterID, fmt.Errorf("rocksdb: decode event: %w", err)
		}

		if len(wantKind) > 0 {
			if _, ok := wantKind[rec.Kind]; !ok {
				continue
			}
		}
		if wantTxID != "" {
			var p struct {
				TxID string `json:"txid"`
			}
			_ = json.Unmarshal([]byte(rec.Payload), &p)
			if strings.ToLower(strings.TrimSpace(p.TxID)) != wantTxID {
				continue
			}
		}

		nextCursor = int64(id)
		out = append(out, store.Event{
			ID:        int64(id),
			Kind:      rec.Kind,
			WalletID:  walletID,
			Height:    rec.Height,
			Payload:   json.RawMessage(rec.Payload),
			CreatedAt: time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, afterID, fmt.Errorf("rocksdb: list events: %w", err)
	}
	return out, nextCursor, nil
}

func (s *Store) ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]store.Note, error) {
	_ = ctx
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyWalletNoteIndexMin(walletID),
		UpperBound: prefixUpperBound(keyWalletNotePrefix(walletID)),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	out := make([]store.Note, 0, limit)
	for iter.First(); iter.Valid(); iter.Next() {
		if len(out) >= limit {
			break
		}

		noteKey, err := noteKeyFromWalletNoteIndex(iter.Key(), walletID)
		if err != nil {
			return nil, err
		}

		v, closer, err := s.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode note: %w", err)
		}
		_ = closer.Close()

		if onlyUnspent && rec.SpentHeight == nil {
			// ok
		} else if onlyUnspent {
			continue
		}

		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		var pendingAt *time.Time
		if rec.PendingSpentAtUnix != nil {
			t := time.Unix(*rec.PendingSpentAtUnix, 0).UTC()
			pendingAt = &t
		}
		out = append(out, store.Note{
			WalletID:             walletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			PendingSpentTxID:     rec.PendingSpentTxID,
			PendingSpentAt:       pendingAt,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      rec.ConfirmedHeight,
			SpentConfirmedHeight: rec.SpentConfirmedHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list notes: %w", err)
	}
	return out, nil
}

func (s *Store) UpdatePendingSpends(ctx context.Context, pending map[string]string, seenAt time.Time) error {
	_ = ctx
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}
	nowUnix := seenAt.Unix()

	desired := make(map[string]string, len(pending))
	for nf, txid := range pending {
		nf = strings.ToLower(strings.TrimSpace(nf))
		txid = strings.ToLower(strings.TrimSpace(txid))
		if nf == "" || txid == "" {
			continue
		}
		desired[nf] = txid
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: pendingNullifierPrefix,
		UpperBound: prefixUpperBound(pendingNullifierPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter pending: %w", err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		nf := string(bytes.TrimPrefix(iter.Key(), pendingNullifierPrefix))
		if _, ok := desired[nf]; ok {
			continue
		}

		// Clear pending markers on the note record, if we still have it.
		locBytes, closer, err := s.db.Get(keyNullifier(nf))
		if err == nil {
			noteKey := append([]byte{}, locBytes...)
			if closer != nil {
				_ = closer.Close()
			}

			v, closer, err := s.db.Get(noteKey)
			if err == nil {
				var rec noteRecord
				if err := json.Unmarshal(v, &rec); err != nil {
					_ = closer.Close()
					_ = iter.Close()
					return fmt.Errorf("rocksdb: decode note: %w", err)
				}
				if closer != nil {
					_ = closer.Close()
				}

				rec.PendingSpentTxID = nil
				rec.PendingSpentAtUnix = nil

				b, err := json.Marshal(rec)
				if err != nil {
					_ = iter.Close()
					return fmt.Errorf("rocksdb: encode note: %w", err)
				}
				if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
					_ = iter.Close()
					return fmt.Errorf("rocksdb: clear pending note: %w", err)
				}
			} else if !errors.Is(err, pebble.ErrNotFound) {
				if closer != nil {
					_ = closer.Close()
				}
				_ = iter.Close()
				return fmt.Errorf("rocksdb: get note: %w", err)
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			if closer != nil {
				_ = closer.Close()
			}
			_ = iter.Close()
			return fmt.Errorf("rocksdb: get nullifier: %w", err)
		}

		keyCopy := append([]byte{}, iter.Key()...)
		if err := batch.Delete(keyCopy, pebble.NoSync); err != nil {
			_ = iter.Close()
			return fmt.Errorf("rocksdb: delete pending key: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return fmt.Errorf("rocksdb: iter pending: %w", err)
	}
	_ = iter.Close()

	for nf, txid := range desired {
		locBytes, closer, err := s.db.Get(keyNullifier(nf))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return fmt.Errorf("rocksdb: get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		if closer != nil {
			_ = closer.Close()
		}

		v, closer, err := s.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			if closer != nil {
				_ = closer.Close()
			}
			return fmt.Errorf("rocksdb: decode note: %w", err)
		}
		if closer != nil {
			_ = closer.Close()
		}

		if rec.SpentHeight != nil {
			continue
		}

		txidCopy := txid
		if rec.PendingSpentTxID == nil || *rec.PendingSpentTxID != txidCopy {
			rec.PendingSpentTxID = &txidCopy
			rec.PendingSpentAtUnix = &nowUnix
		} else if rec.PendingSpentAtUnix == nil {
			rec.PendingSpentAtUnix = &nowUnix
		}

		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: encode note: %w", err)
		}
		if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: set pending note: %w", err)
		}
		if err := batch.Set(keyPendingNullifier(nf), []byte(txidCopy), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: set pending key: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: update pending commit: %w", err)
	}
	return nil
}

func (s *Store) ListNotesByPendingSpentTxIDs(ctx context.Context, txids []string) ([]store.Note, error) {
	_ = ctx

	txids = cleanStringsLower(txids)
	if len(txids) == 0 {
		return nil, nil
	}

	want := make(map[string]struct{}, len(txids))
	for _, txid := range txids {
		want[txid] = struct{}{}
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: pendingNullifierPrefix,
		UpperBound: prefixUpperBound(pendingNullifierPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter pending: %w", err)
	}
	defer iter.Close()

	var out []store.Note
	for iter.First(); iter.Valid(); iter.Next() {
		txid := string(iter.Value())
		if _, ok := want[txid]; !ok {
			continue
		}

		nf := string(bytes.TrimPrefix(iter.Key(), pendingNullifierPrefix))
		locBytes, closer, err := s.db.Get(keyNullifier(nf))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		if closer != nil {
			_ = closer.Close()
		}

		v, closer, err := s.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			if closer != nil {
				_ = closer.Close()
			}
			return nil, fmt.Errorf("rocksdb: decode note: %w", err)
		}
		if closer != nil {
			_ = closer.Close()
		}

		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		var pendingAt *time.Time
		if rec.PendingSpentAtUnix != nil {
			t := time.Unix(*rec.PendingSpentAtUnix, 0).UTC()
			pendingAt = &t
		}

		out = append(out, store.Note{
			WalletID:             rec.WalletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			PendingSpentTxID:     rec.PendingSpentTxID,
			PendingSpentAt:       pendingAt,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      rec.ConfirmedHeight,
			SpentConfirmedHeight: rec.SpentConfirmedHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: iter pending: %w", err)
	}

	return out, nil
}

func (s *Store) ListOutgoingOutputsByTxID(ctx context.Context, walletID, txid string) ([]store.OutgoingOutput, error) {
	_ = ctx

	walletID = strings.TrimSpace(walletID)
	txid = strings.ToLower(strings.TrimSpace(txid))
	if walletID == "" || txid == "" {
		return nil, errors.New("rocksdb: wallet_id and txid are required")
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyOutgoingOutputTxPrefix(walletID, txid),
		UpperBound: prefixUpperBound(keyOutgoingOutputTxPrefix(walletID, txid)),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter outgoing outputs: %w", err)
	}
	defer iter.Close()

	var out []store.OutgoingOutput
	for iter.First(); iter.Valid(); iter.Next() {
		var rec outgoingOutputRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode outgoing output: %w", err)
		}

		var minedHeight *int64
		if rec.MinedHeight != nil {
			h := *rec.MinedHeight
			minedHeight = &h
		}
		var confirmedHeight *int64
		if rec.ConfirmedHeight != nil {
			h := *rec.ConfirmedHeight
			confirmedHeight = &h
		}
		var mempoolSeenAt *time.Time
		if rec.MempoolSeenUnix != nil {
			t := time.Unix(*rec.MempoolSeenUnix, 0).UTC()
			mempoolSeenAt = &t
		}
		out = append(out, store.OutgoingOutput{
			WalletID:         rec.WalletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      minedHeight,
			ConfirmedHeight:  confirmedHeight,
			MempoolSeenAt:    mempoolSeenAt,
			RecipientAddress: rec.RecipientAddress,
			ValueZat:         rec.ValueZat,
			MemoHex:          rec.MemoHex,
			OvkScope:         rec.OvkScope,
			RecipientScope:   rec.RecipientScope,
			CreatedAt:        time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list outgoing outputs: %w", err)
	}
	return out, nil
}

func (s *Store) ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]store.OrchardCommitment, error) {
	_ = ctx
	if height < 0 {
		return nil, nil
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: commitmentPrefix,
		UpperBound: prefixUpperBound(commitmentPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.OrchardCommitment
	for iter.First(); iter.Valid(); iter.Next() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		if rec.Height > height {
			break
		}
		out = append(out, store.OrchardCommitment{
			Position:    int64(rec.Position),
			Height:      rec.Height,
			TxID:        rec.TxID,
			ActionIndex: rec.ActionIndex,
			CMX:         rec.CMX,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list commitments: %w", err)
	}
	return out, nil
}

func (s *Store) FirstOrchardCommitmentPositionFromHeight(ctx context.Context, height int64) (int64, bool, error) {
	_ = ctx
	if height < 0 {
		height = 0
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: commitmentPrefix,
		UpperBound: prefixUpperBound(commitmentPrefix),
	})
	if err != nil {
		return 0, false, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return 0, false, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		if rec.Height >= height {
			return int64(rec.Position), true, nil
		}
	}
	if err := iter.Error(); err != nil {
		return 0, false, fmt.Errorf("rocksdb: iter: %w", err)
	}
	return 0, false, nil
}

type rocksTx struct {
	db    *pebble.DB
	batch *pebble.Batch
	now   time.Time

	nextPosLoaded bool
	nextPos       uint64
	maxPos        uint64
}

func (t *rocksTx) InsertBlock(ctx context.Context, b store.Block) error {
	_ = ctx
	if b.Height < 0 {
		return errors.New("rocksdb: negative height")
	}

	key := keyBlock(b.Height)
	if _, closer, err := t.db.Get(key); err == nil {
		_ = closer.Close()
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get block: %w", err)
	}

	rec := blockRecord{
		Hash:        b.Hash,
		PrevHash:    b.PrevHash,
		Time:        b.Time,
		ScannedUnix: t.now.Unix(),
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode block: %w", err)
	}
	if err := t.batch.Set(key, v, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert block: %w", err)
	}
	return nil
}

func (t *rocksTx) NextOrchardCommitmentPosition(ctx context.Context) (int64, error) {
	_ = ctx

	if t.nextPosLoaded {
		return int64(t.nextPos), nil
	}

	var next uint64
	v, closer, err := t.db.Get(keyMeta("next_commitment_pos"))
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return 0, errors.New("rocksdb: next_commitment_pos corrupt")
		}
		next = binary.BigEndian.Uint64(v)
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return 0, fmt.Errorf("rocksdb: get next_commitment_pos: %w", err)
	}

	t.nextPosLoaded = true
	t.nextPos = next
	t.maxPos = next
	return int64(next), nil
}

func (t *rocksTx) InsertOrchardAction(ctx context.Context, a store.OrchardAction) error {
	_ = ctx
	if a.Height < 0 {
		return errors.New("rocksdb: negative height")
	}

	key := keyOrchardAction(a.TxID, a.ActionIndex)
	if _, closer, err := t.db.Get(key); err == nil {
		_ = closer.Close()
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get action: %w", err)
	}

	rec := orchardActionRecord{
		Height:          a.Height,
		TxID:            a.TxID,
		ActionIndex:     a.ActionIndex,
		ActionNullifier: a.ActionNullifier,
		CMX:             a.CMX,
		EphemeralKey:    a.EphemeralKey,
		EncCiphertext:   a.EncCiphertext,
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode action: %w", err)
	}
	if err := t.batch.Set(key, v, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert action: %w", err)
	}

	if err := t.batch.Set(keyOrchardActionHeightIndex(uint64(a.Height), a.TxID, a.ActionIndex), nil, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert action index: %w", err)
	}
	return nil
}

func (t *rocksTx) InsertOrchardCommitment(ctx context.Context, c store.OrchardCommitment) error {
	_ = ctx
	if c.Position < 0 {
		return errors.New("rocksdb: negative position")
	}
	if c.Height < 0 {
		return errors.New("rocksdb: negative height")
	}

	key := keyCommitment(uint64(c.Position))
	if _, closer, err := t.db.Get(key); err == nil {
		_ = closer.Close()
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get commitment: %w", err)
	}

	rec := commitmentRecord{
		Position:    uint64(c.Position),
		Height:      c.Height,
		TxID:        c.TxID,
		ActionIndex: c.ActionIndex,
		CMX:         c.CMX,
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode commitment: %w", err)
	}
	if err := t.batch.Set(key, v, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert commitment: %w", err)
	}

	if t.nextPosLoaded && uint64(c.Position)+1 > t.maxPos {
		t.maxPos = uint64(c.Position) + 1
	}
	if err := t.batch.Set(keyMeta("next_commitment_pos"), uint64To8(t.maxPos), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: bump next_commitment_pos: %w", err)
	}
	return nil
}

func (t *rocksTx) MarkNotesSpent(ctx context.Context, height int64, txid string, nullifiers []string) ([]store.Note, error) {
	_ = ctx
	if height < 0 {
		return nil, errors.New("rocksdb: negative height")
	}
	if len(nullifiers) == 0 {
		return nil, nil
	}

	var out []store.Note

	for _, nf := range nullifiers {
		locBytes, closer, err := t.db.Get(keyNullifier(nf))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := t.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode note: %w", err)
		}
		_ = closer.Close()

		if rec.SpentHeight != nil {
			continue
		}
		rec.SpentHeight = &height
		rec.SpentTxID = &txid
		rec.SpentConfirmedHeight = nil
		rec.PendingSpentTxID = nil
		rec.PendingSpentAtUnix = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: encode note: %w", err)
		}
		if err := t.batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: mark spent: %w", err)
		}
		if err := t.batch.Delete(keyPendingNullifier(nf), pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: clear pending key: %w", err)
		}
		if err := t.batch.Set(keySpentHeightIndex(uint64(height), nf), nil, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: mark spent index: %w", err)
		}

		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		out = append(out, store.Note{
			WalletID:             rec.WalletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      rec.ConfirmedHeight,
			SpentConfirmedHeight: rec.SpentConfirmedHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	return out, nil
}

func (t *rocksTx) ConfirmNotes(ctx context.Context, confirmationHeight int64, maxNoteHeight int64) ([]store.Note, error) {
	_ = ctx
	if confirmationHeight < 0 {
		return nil, errors.New("rocksdb: negative confirmation height")
	}
	if maxNoteHeight < 0 {
		return nil, nil
	}

	var startHeight uint64 = 0
	v, closer, err := t.batch.Get(keyMeta("confirmed_upto_note_height"))
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return nil, errors.New("rocksdb: confirmed_upto_note_height corrupt")
		}
		startHeight = binary.BigEndian.Uint64(v) + 1
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("rocksdb: get confirmed_upto_note_height: %w", err)
	}

	if startHeight > uint64(maxNoteHeight) {
		startHeight = 0
	}

	lower := make([]byte, 0, len(noteHeightPrefix)+20)
	lower = append(lower, noteHeightPrefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteHeightPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.Note
	for iter.First(); iter.Valid(); iter.Next() {
		walletID, height, txid, actionIndex, err := parseNoteHeightIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		if height > maxNoteHeight {
			break
		}

		noteKey := keyNote(walletID, txid, actionIndex)
		v, closer, err := t.batch.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode note: %w", err)
		}
		_ = closer.Close()

		if rec.ConfirmedHeight != nil {
			continue
		}
		rec.ConfirmedHeight = &confirmationHeight

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: encode note: %w", err)
		}
		if err := t.batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: confirm note: %w", err)
		}
		if rec.NoteNullifier != "" {
			if err := t.batch.Set(keyConfirmedHeightIndex(uint64(confirmationHeight), rec.NoteNullifier), nil, pebble.NoSync); err != nil {
				return nil, fmt.Errorf("rocksdb: confirm note index: %w", err)
			}
		}

		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		out = append(out, store.Note{
			WalletID:             walletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      &confirmationHeight,
			SpentConfirmedHeight: rec.SpentConfirmedHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: confirm notes iter: %w", err)
	}

	if err := t.batch.Set(keyMeta("confirmed_upto_note_height"), uint64To8(uint64(maxNoteHeight)), pebble.NoSync); err != nil {
		return nil, fmt.Errorf("rocksdb: set confirmed_upto_note_height: %w", err)
	}

	return out, nil
}

func (t *rocksTx) ConfirmOutgoingOutputs(ctx context.Context, confirmationHeight int64, maxMinedHeight int64) ([]store.OutgoingOutput, error) {
	_ = ctx
	if confirmationHeight < 0 {
		return nil, errors.New("rocksdb: negative confirmation height")
	}
	if maxMinedHeight < 0 {
		return nil, nil
	}

	var startHeight uint64 = 0
	v, closer, err := t.batch.Get(keyMeta("confirmed_upto_outgoing_mined_height"))
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return nil, errors.New("rocksdb: confirmed_upto_outgoing_mined_height corrupt")
		}
		startHeight = binary.BigEndian.Uint64(v) + 1
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("rocksdb: get confirmed_upto_outgoing_mined_height: %w", err)
	}

	if startHeight > uint64(maxMinedHeight) {
		startHeight = 0
	}

	lower := make([]byte, 0, len(outgoingMinedHeightPrefix)+20)
	lower = append(lower, outgoingMinedHeightPrefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(outgoingMinedHeightPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter outgoing confirmations: %w", err)
	}
	defer iter.Close()

	var out []store.OutgoingOutput
	for iter.First(); iter.Valid(); iter.Next() {
		walletID, minedHeight, txid, actionIndex, err := parseOutgoingMinedHeightIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		if minedHeight > maxMinedHeight {
			break
		}

		ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
		v, closer, err := t.batch.Get(ooKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get outgoing output: %w", err)
		}
		var rec outgoingOutputRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode outgoing output: %w", err)
		}
		_ = closer.Close()

		if rec.ConfirmedHeight != nil {
			continue
		}
		rec.ConfirmedHeight = &confirmationHeight

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: encode outgoing output: %w", err)
		}
		if err := t.batch.Set(ooKey, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: confirm outgoing output: %w", err)
		}
		if err := t.batch.Set(keyOutgoingConfirmedHeightIndex(uint64(confirmationHeight), walletID, txid, actionIndex), nil, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: confirm outgoing output index: %w", err)
		}

		var minedHeightPtr *int64
		if rec.MinedHeight != nil {
			h := *rec.MinedHeight
			minedHeightPtr = &h
		}
		var mempoolSeenAt *time.Time
		if rec.MempoolSeenUnix != nil {
			tm := time.Unix(*rec.MempoolSeenUnix, 0).UTC()
			mempoolSeenAt = &tm
		}
		out = append(out, store.OutgoingOutput{
			WalletID:         walletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      minedHeightPtr,
			ConfirmedHeight:  &confirmationHeight,
			MempoolSeenAt:    mempoolSeenAt,
			RecipientAddress: rec.RecipientAddress,
			ValueZat:         rec.ValueZat,
			MemoHex:          rec.MemoHex,
			OvkScope:         rec.OvkScope,
			RecipientScope:   rec.RecipientScope,
			CreatedAt:        time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: confirm outgoing outputs iter: %w", err)
	}

	if err := t.batch.Set(keyMeta("confirmed_upto_outgoing_mined_height"), uint64To8(uint64(maxMinedHeight)), pebble.NoSync); err != nil {
		return nil, fmt.Errorf("rocksdb: set confirmed_upto_outgoing_mined_height: %w", err)
	}
	return out, nil
}

func (t *rocksTx) ConfirmSpends(ctx context.Context, confirmationHeight int64, maxSpentHeight int64) ([]store.Note, error) {
	_ = ctx
	if confirmationHeight < 0 {
		return nil, errors.New("rocksdb: negative confirmation height")
	}
	if maxSpentHeight < 0 {
		return nil, nil
	}

	var startHeight uint64 = 0
	v, closer, err := t.batch.Get(keyMeta("confirmed_upto_spent_height"))
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return nil, errors.New("rocksdb: confirmed_upto_spent_height corrupt")
		}
		startHeight = binary.BigEndian.Uint64(v) + 1
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("rocksdb: get confirmed_upto_spent_height: %w", err)
	}

	if startHeight > uint64(maxSpentHeight) {
		startHeight = 0
	}

	lower := make([]byte, 0, len(noteSpentHeightPrefix)+20)
	lower = append(lower, noteSpentHeightPrefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteSpentHeightPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	var out []store.Note
	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)
		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 3 {
			continue
		}
		spentHeight, err := parseFixed20Int64(parts[1])
		if err != nil {
			return nil, fmt.Errorf("rocksdb: parse spent height: %w", err)
		}
		if spentHeight > maxSpentHeight {
			break
		}
		nullifier := string(parts[2])

		locBytes, closer, err := t.batch.Get(keyNullifier(nullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = t.batch.Delete(k, pebble.NoSync)
				continue
			}
			return nil, fmt.Errorf("rocksdb: confirm spend get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := t.batch.Get(noteKey)
		if err != nil {
			_ = t.batch.Delete(k, pebble.NoSync)
			continue
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: confirm spend decode note: %w", err)
		}
		_ = closer.Close()

		if rec.SpentHeight == nil {
			_ = t.batch.Delete(k, pebble.NoSync)
			continue
		}
		if rec.SpentConfirmedHeight != nil {
			continue
		}

		rec.SpentConfirmedHeight = &confirmationHeight

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: confirm spend encode note: %w", err)
		}
		if err := t.batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: confirm spend set note: %w", err)
		}
		if rec.NoteNullifier != "" {
			if err := t.batch.Set(keySpentConfirmedHeightIndex(uint64(confirmationHeight), rec.NoteNullifier), nil, pebble.NoSync); err != nil {
				return nil, fmt.Errorf("rocksdb: confirm spend index: %w", err)
			}
		}

		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		out = append(out, store.Note{
			WalletID:             rec.WalletID,
			TxID:                 rec.TxID,
			ActionIndex:          rec.ActionIndex,
			Height:               rec.Height,
			Position:             posPtr,
			DiversifierIndex:     rec.DiversifierIndex,
			RecipientAddress:     rec.RecipientAddress,
			ValueZat:             rec.ValueZat,
			MemoHex:              rec.MemoHex,
			NoteNullifier:        rec.NoteNullifier,
			SpentHeight:          rec.SpentHeight,
			SpentTxID:            rec.SpentTxID,
			ConfirmedHeight:      rec.ConfirmedHeight,
			SpentConfirmedHeight: &confirmationHeight,
			CreatedAt:            time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: confirm spends iter: %w", err)
	}

	if err := t.batch.Set(keyMeta("confirmed_upto_spent_height"), uint64To8(uint64(maxSpentHeight)), pebble.NoSync); err != nil {
		return nil, fmt.Errorf("rocksdb: set confirmed_upto_spent_height: %w", err)
	}

	return out, nil
}

func (t *rocksTx) InsertNote(ctx context.Context, n store.Note) (bool, error) {
	_ = ctx
	if n.Height < 0 {
		return false, errors.New("rocksdb: negative height")
	}

	key := keyNote(n.WalletID, n.TxID, n.ActionIndex)
	if _, closer, err := t.db.Get(key); err == nil {
		_ = closer.Close()
		return false, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return false, fmt.Errorf("rocksdb: get note: %w", err)
	}

	rec := noteRecord{
		WalletID:         n.WalletID,
		TxID:             n.TxID,
		ActionIndex:      n.ActionIndex,
		Height:           n.Height,
		DiversifierIndex: n.DiversifierIndex,
		RecipientAddress: n.RecipientAddress,
		ValueZat:         n.ValueZat,
		MemoHex:          n.MemoHex,
		NoteNullifier:    n.NoteNullifier,
		CreatedAtUnix:    t.now.Unix(),
	}
	if n.Position != nil {
		pos := uint64(*n.Position)
		rec.Position = &pos
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return false, fmt.Errorf("rocksdb: encode note: %w", err)
	}
	if err := t.batch.Set(key, b, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert note: %w", err)
	}

	if err := t.batch.Set(keyNullifier(n.NoteNullifier), key, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert nullifier: %w", err)
	}
	if err := t.batch.Set(keyNoteHeightIndex(uint64(n.Height), n.WalletID, n.TxID, n.ActionIndex), []byte(n.NoteNullifier), pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert note height index: %w", err)
	}
	if err := t.batch.Set(keyWalletNoteIndex(uint64(n.Height), n.WalletID, n.TxID, n.ActionIndex), nil, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert wallet note index: %w", err)
	}
	return true, nil
}

func (t *rocksTx) InsertOutgoingOutput(ctx context.Context, o store.OutgoingOutput) (bool, error) {
	_ = ctx

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
		return false, errors.New("rocksdb: outgoing output missing required fields")
	}

	key := keyOutgoingOutput(o.WalletID, o.TxID, o.ActionIndex)
	v, closer, err := t.batch.Get(key)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return false, fmt.Errorf("rocksdb: get outgoing output: %w", err)
	}
	if err == nil {
		defer func() { _ = closer.Close() }()
		var rec outgoingOutputRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return false, fmt.Errorf("rocksdb: decode outgoing output: %w", err)
		}

		changed := false
		if rec.MinedHeight == nil && o.MinedHeight != nil {
			h := *o.MinedHeight
			rec.MinedHeight = &h
			if err := t.batch.Set(keyOutgoingMinedHeightIndex(uint64(h), o.WalletID, o.TxID, o.ActionIndex), nil, pebble.NoSync); err != nil {
				return false, fmt.Errorf("rocksdb: insert outgoing mined height index: %w", err)
			}
			changed = true
		}
		if rec.MempoolSeenUnix == nil && o.MempoolSeenAt != nil && !o.MempoolSeenAt.IsZero() {
			u := o.MempoolSeenAt.UTC().Unix()
			rec.MempoolSeenUnix = &u
			changed = true
		}
		if !changed {
			return false, nil
		}

		b, err := json.Marshal(rec)
		if err != nil {
			return false, fmt.Errorf("rocksdb: encode outgoing output: %w", err)
		}
		if err := t.batch.Set(key, b, pebble.NoSync); err != nil {
			return false, fmt.Errorf("rocksdb: update outgoing output: %w", err)
		}
		return true, nil
	}

	rec := outgoingOutputRecord{
		WalletID:         o.WalletID,
		TxID:             o.TxID,
		ActionIndex:      o.ActionIndex,
		MinedHeight:      o.MinedHeight,
		RecipientAddress: o.RecipientAddress,
		ValueZat:         o.ValueZat,
		MemoHex:          o.MemoHex,
		OvkScope:         o.OvkScope,
		RecipientScope:   o.RecipientScope,
		CreatedAtUnix:    t.now.Unix(),
	}
	if o.MempoolSeenAt != nil && !o.MempoolSeenAt.IsZero() {
		u := o.MempoolSeenAt.UTC().Unix()
		rec.MempoolSeenUnix = &u
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return false, fmt.Errorf("rocksdb: encode outgoing output: %w", err)
	}
	if err := t.batch.Set(key, b, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert outgoing output: %w", err)
	}
	if o.MinedHeight != nil {
		if err := t.batch.Set(keyOutgoingMinedHeightIndex(uint64(*o.MinedHeight), o.WalletID, o.TxID, o.ActionIndex), nil, pebble.NoSync); err != nil {
			return false, fmt.Errorf("rocksdb: insert outgoing mined height index: %w", err)
		}
	}
	return true, nil
}

func (t *rocksTx) InsertEvent(ctx context.Context, e store.Event) error {
	_ = ctx
	if e.Height < 0 {
		return errors.New("rocksdb: negative height")
	}

	walletID := e.WalletID

	seqKey := keyEventSeq(walletID)
	var nextID uint64 = 1
	v, closer, err := t.batch.Get(seqKey)
	if err == nil {
		if len(v) != 8 {
			_ = closer.Close()
			return errors.New("rocksdb: event seq corrupt")
		}
		nextID = binary.BigEndian.Uint64(v)
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get event seq: %w", err)
	}

	rec := eventRecord{
		Kind:          e.Kind,
		WalletID:      walletID,
		Height:        e.Height,
		Payload:       string(e.Payload),
		CreatedAtUnix: t.now.Unix(),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode event: %w", err)
	}
	if err := t.batch.Set(keyEvent(walletID, nextID), b, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert event: %w", err)
	}
	if err := t.batch.Set(keyEventHeightIndex(uint64(e.Height), walletID, nextID), nil, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert event height index: %w", err)
	}

	if err := t.batch.Set(seqKey, uint64To8(nextID+1), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: bump event seq: %w", err)
	}
	return nil
}

type walletRecord struct {
	UFVK           string `json:"ufvk"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
	DisabledAtUnix *int64 `json:"disabled_at_unix,omitempty"`
}

type blockRecord struct {
	Hash        string `json:"hash"`
	PrevHash    string `json:"prev_hash,omitempty"`
	Time        int64  `json:"time"`
	ScannedUnix int64  `json:"scanned_at_unix"`
}

type orchardActionRecord struct {
	Height          int64  `json:"height"`
	TxID            string `json:"txid"`
	ActionIndex     int32  `json:"action_index"`
	ActionNullifier string `json:"action_nullifier"`
	CMX             string `json:"cmx"`
	EphemeralKey    string `json:"ephemeral_key"`
	EncCiphertext   string `json:"enc_ciphertext"`
}

type commitmentRecord struct {
	Position    uint64 `json:"position"`
	Height      int64  `json:"height"`
	TxID        string `json:"txid"`
	ActionIndex int32  `json:"action_index"`
	CMX         string `json:"cmx"`
}

type noteRecord struct {
	WalletID             string  `json:"wallet_id"`
	TxID                 string  `json:"txid"`
	ActionIndex          int32   `json:"action_index"`
	Height               int64   `json:"height"`
	Position             *uint64 `json:"position,omitempty"`
	DiversifierIndex     uint32  `json:"diversifier_index,omitempty"`
	RecipientAddress     string  `json:"recipient_address"`
	ValueZat             int64   `json:"value_zat"`
	MemoHex              *string `json:"memo_hex,omitempty"`
	NoteNullifier        string  `json:"note_nullifier"`
	PendingSpentTxID     *string `json:"pending_spent_txid,omitempty"`
	PendingSpentAtUnix   *int64  `json:"pending_spent_at_unix,omitempty"`
	SpentHeight          *int64  `json:"spent_height,omitempty"`
	SpentTxID            *string `json:"spent_txid,omitempty"`
	ConfirmedHeight      *int64  `json:"confirmed_height,omitempty"`
	SpentConfirmedHeight *int64  `json:"spent_confirmed_height,omitempty"`
	CreatedAtUnix        int64   `json:"created_at_unix"`
}

type outgoingOutputRecord struct {
	WalletID string `json:"wallet_id"`
	TxID     string `json:"txid"`

	ActionIndex int32 `json:"action_index"`

	MinedHeight     *int64 `json:"mined_height,omitempty"`
	ConfirmedHeight *int64 `json:"confirmed_height,omitempty"`
	MempoolSeenUnix *int64 `json:"mempool_seen_at_unix,omitempty"`

	RecipientAddress string  `json:"recipient_address"`
	ValueZat         int64   `json:"value_zat"`
	MemoHex          *string `json:"memo_hex,omitempty"`

	OvkScope       string  `json:"ovk_scope"`
	RecipientScope *string `json:"recipient_scope,omitempty"`

	CreatedAtUnix int64 `json:"created_at_unix"`
}

type eventRecord struct {
	Kind          string `json:"kind"`
	WalletID      string `json:"wallet_id"`
	Height        int64  `json:"height"`
	Payload       string `json:"payload"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

var (
	walletPrefix                   = []byte("w/")
	blockPrefix                    = []byte("b/")
	orchardActionPrefix            = []byte("oa/")
	orchardActionHeightPrefix      = []byte("oah/")
	commitmentPrefix               = []byte("oc/")
	notePrefix                     = []byte("n/")
	noteHeightPrefix               = []byte("nh/")
	walletNotePrefix               = []byte("nwh/")
	nullifierPrefix                = []byte("nn/")
	pendingNullifierPrefix         = []byte("np/")
	noteSpentHeightPrefix          = []byte("nsh/")
	noteConfirmedHeightPrefix      = []byte("nch/")
	noteSpentConfirmedHeightPrefix = []byte("nsch/")
	outgoingOutputPrefix           = []byte("oo/")
	outgoingMinedHeightPrefix      = []byte("oomh/")
	outgoingConfirmedHeightPrefix  = []byte("ooch/")
	eventPrefix                    = []byte("e/")
	eventHeightPrefix              = []byte("eh/")
	eventSeqPrefix                 = []byte("es/")
	metaPrefix                     = []byte("m/")
)

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte{}, prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return []byte{0xFF}
}

func keyMeta(name string) []byte {
	b := make([]byte, 0, len(metaPrefix)+len(name))
	b = append(b, metaPrefix...)
	b = append(b, name...)
	return b
}

func keyWallet(walletID string) []byte {
	b := make([]byte, 0, len(walletPrefix)+len(walletID))
	b = append(b, walletPrefix...)
	b = append(b, walletID...)
	return b
}

func keyBlock(height int64) []byte {
	b := make([]byte, 0, len(blockPrefix)+20)
	b = append(b, blockPrefix...)
	b = appendUint64Fixed20(b, uint64(height))
	return b
}

func keyOrchardAction(txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(orchardActionPrefix)+len(txid)+1+11)
	b = append(b, orchardActionPrefix...)
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyOrchardActionHeightIndex(height uint64, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(orchardActionHeightPrefix)+20+1+len(txid)+1+11)
	b = append(b, orchardActionHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyCommitment(position uint64) []byte {
	b := make([]byte, 0, len(commitmentPrefix)+20)
	b = append(b, commitmentPrefix...)
	b = appendUint64Fixed20(b, position)
	return b
}

func keyNote(walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(notePrefix)+len(walletID)+1+len(txid)+1+11)
	b = append(b, notePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyOutgoingOutput(walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(outgoingOutputPrefix)+len(walletID)+1+len(txid)+1+11)
	b = append(b, outgoingOutputPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyOutgoingOutputTxPrefix(walletID, txid string) []byte {
	b := make([]byte, 0, len(outgoingOutputPrefix)+len(walletID)+1+len(txid)+1)
	b = append(b, outgoingOutputPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	return b
}

func keyNullifier(nf string) []byte {
	b := make([]byte, 0, len(nullifierPrefix)+len(nf))
	b = append(b, nullifierPrefix...)
	b = append(b, nf...)
	return b
}

func keyPendingNullifier(nf string) []byte {
	b := make([]byte, 0, len(pendingNullifierPrefix)+len(nf))
	b = append(b, pendingNullifierPrefix...)
	b = append(b, nf...)
	return b
}

func keyNoteHeightIndex(height uint64, walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(noteHeightPrefix)+20+1+len(walletID)+1+len(txid)+1+11)
	b = append(b, noteHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyWalletNotePrefix(walletID string) []byte {
	b := make([]byte, 0, len(walletNotePrefix)+len(walletID)+1)
	b = append(b, walletNotePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	return b
}

func keyWalletNoteIndexMin(walletID string) []byte {
	b := make([]byte, 0, len(walletNotePrefix)+len(walletID)+1+20)
	b = append(b, walletNotePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = appendUint64Fixed20(b, 0)
	return b
}

func keyWalletNoteIndex(height uint64, walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(walletNotePrefix)+len(walletID)+1+20+1+len(txid)+1+11)
	b = append(b, walletNotePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keySpentHeightIndex(height uint64, nullifier string) []byte {
	b := make([]byte, 0, len(noteSpentHeightPrefix)+20+1+len(nullifier))
	b = append(b, noteSpentHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, nullifier...)
	return b
}

func keyConfirmedHeightIndex(height uint64, nullifier string) []byte {
	b := make([]byte, 0, len(noteConfirmedHeightPrefix)+20+1+len(nullifier))
	b = append(b, noteConfirmedHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, nullifier...)
	return b
}

func keySpentConfirmedHeightIndex(height uint64, nullifier string) []byte {
	b := make([]byte, 0, len(noteSpentConfirmedHeightPrefix)+20+1+len(nullifier))
	b = append(b, noteSpentConfirmedHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, nullifier...)
	return b
}

func keyOutgoingMinedHeightIndex(height uint64, walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(outgoingMinedHeightPrefix)+20+1+len(walletID)+1+len(txid)+1+11)
	b = append(b, outgoingMinedHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyOutgoingConfirmedHeightIndex(height uint64, walletID, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(outgoingConfirmedHeightPrefix)+20+1+len(walletID)+1+len(txid)+1+11)
	b = append(b, outgoingConfirmedHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyEventSeq(walletID string) []byte {
	b := make([]byte, 0, len(eventSeqPrefix)+len(walletID))
	b = append(b, eventSeqPrefix...)
	b = append(b, walletID...)
	return b
}

func keyEvent(walletID string, id uint64) []byte {
	b := make([]byte, 0, len(eventPrefix)+len(walletID)+1+20)
	b = append(b, eventPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = appendUint64Fixed20(b, id)
	return b
}

func keyEventHeightIndex(height uint64, walletID string, id uint64) []byte {
	b := make([]byte, 0, len(eventHeightPrefix)+20+1+len(walletID)+1+20)
	b = append(b, eventHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, walletID...)
	b = append(b, '/')
	b = appendUint64Fixed20(b, id)
	return b
}

func appendUint64Fixed20(dst []byte, n uint64) []byte {
	var buf [20]byte
	for i := 19; i >= 0; i-- {
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return append(dst, buf[:]...)
}

func parseFixed20Int64(b []byte) (int64, error) {
	if len(b) != 20 {
		return 0, errors.New("invalid fixed20")
	}
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	if n > uint64(^uint64(0)>>1) {
		return 0, errors.New("overflow")
	}
	return int64(n), nil
}

func uint64To8(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func deleteRangeByFixed20(batch *pebble.Batch, prefix []byte, start uint64) error {
	lower := make([]byte, 0, len(prefix)+20)
	lower = append(lower, prefix...)
	lower = appendUint64Fixed20(lower, start)
	upper := prefixUpperBound(prefix)
	if err := batch.DeleteRange(lower, upper, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: delete range: %w", err)
	}
	return nil
}

func deleteIndexedByHeight(batch *pebble.Batch, db *pebble.DB, prefix []byte, startHeight uint64, extraDeletes func(key []byte) [][]byte) error {
	lower := make([]byte, 0, len(prefix)+20)
	lower = append(lower, prefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)
		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete index: %w", err)
		}
		for _, dk := range extraDeletes(k) {
			if err := batch.Delete(dk, pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete data: %w", err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	return nil
}

func deleteNotesAboveHeight(batch *pebble.Batch, db *pebble.DB, startHeight uint64) error {
	lower := make([]byte, 0, len(noteHeightPrefix)+20)
	lower = append(lower, noteHeightPrefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)
		nullifier := string(iter.Value())

		walletID, height, txid, actionIndex, err := parseNoteHeightIndexKey(k)
		if err != nil {
			return err
		}

		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete note height index: %w", err)
		}
		if err := batch.Delete(keyWalletNoteIndex(uint64(height), walletID, txid, actionIndex), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete wallet note index: %w", err)
		}
		if err := batch.Delete(keyNote(walletID, txid, actionIndex), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete note: %w", err)
		}
		if nullifier != "" {
			if err := batch.Delete(keyNullifier(nullifier), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete nullifier: %w", err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: delete notes iter: %w", err)
	}
	return nil
}

func unspendNotesAboveHeight(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	lower := make([]byte, 0, len(noteSpentHeightPrefix)+20)
	lower = append(lower, noteSpentHeightPrefix...)
	lower = appendUint64Fixed20(lower, height+1)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteSpentHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)

		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 3 {
			continue
		}
		nullifier := string(parts[2])

		locBytes, closer, err := batch.Get(keyNullifier(nullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: unspend get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := batch.Get(noteKey)
		if err != nil {
			_ = batch.Delete(k, pebble.NoSync)
			continue
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unspend decode note: %w", err)
		}
		_ = closer.Close()

		rec.SpentHeight = nil
		rec.SpentTxID = nil
		rec.SpentConfirmedHeight = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: unspend encode note: %w", err)
		}
		if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unspend set note: %w", err)
		}
		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unspend delete index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: unspend iter: %w", err)
	}
	return nil
}

func unconfirmNotesAboveHeight(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	lower := make([]byte, 0, len(noteConfirmedHeightPrefix)+20)
	lower = append(lower, noteConfirmedHeightPrefix...)
	lower = appendUint64Fixed20(lower, height+1)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteConfirmedHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)

		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 3 {
			continue
		}
		nullifier := string(parts[2])

		locBytes, closer, err := batch.Get(keyNullifier(nullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: unconfirm get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := batch.Get(noteKey)
		if err != nil {
			_ = batch.Delete(k, pebble.NoSync)
			continue
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unconfirm decode note: %w", err)
		}
		_ = closer.Close()

		rec.ConfirmedHeight = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: unconfirm encode note: %w", err)
		}
		if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm set note: %w", err)
		}
		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm delete index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: unconfirm iter: %w", err)
	}
	return nil
}

func unconfirmSpendsAboveHeight(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	lower := make([]byte, 0, len(noteSpentConfirmedHeightPrefix)+20)
	lower = append(lower, noteSpentConfirmedHeightPrefix...)
	lower = appendUint64Fixed20(lower, height+1)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(noteSpentConfirmedHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)

		parts := bytes.Split(k, []byte("/"))
		if len(parts) != 3 {
			continue
		}
		nullifier := string(parts[2])

		locBytes, closer, err := batch.Get(keyNullifier(nullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: unconfirm spend get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := batch.Get(noteKey)
		if err != nil {
			_ = batch.Delete(k, pebble.NoSync)
			continue
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unconfirm spend decode note: %w", err)
		}
		_ = closer.Close()

		rec.SpentConfirmedHeight = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: unconfirm spend encode note: %w", err)
		}
		if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm spend set note: %w", err)
		}
		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm spend delete index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: unconfirm spends iter: %w", err)
	}
	return nil
}

func deleteOutgoingOutputsAboveHeight(batch *pebble.Batch, db *pebble.DB, startHeight uint64) error {
	lower := make([]byte, 0, len(outgoingMinedHeightPrefix)+20)
	lower = append(lower, outgoingMinedHeightPrefix...)
	lower = appendUint64Fixed20(lower, startHeight)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(outgoingMinedHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)

		walletID, _, txid, actionIndex, err := parseOutgoingMinedHeightIndexKey(k)
		if err != nil {
			return err
		}

		ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
		v, closer, err := batch.Get(ooKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: delete outgoing output get: %w", err)
		}
		var rec outgoingOutputRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: delete outgoing output decode: %w", err)
		}
		_ = closer.Close()

		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete outgoing mined index: %w", err)
		}
		if err := batch.Delete(ooKey, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete outgoing output: %w", err)
		}
		if rec.ConfirmedHeight != nil {
			if err := batch.Delete(keyOutgoingConfirmedHeightIndex(uint64(*rec.ConfirmedHeight), walletID, txid, actionIndex), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete outgoing confirmed index: %w", err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: delete outgoing outputs iter: %w", err)
	}
	return nil
}

func unconfirmOutgoingOutputsAboveHeight(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	lower := make([]byte, 0, len(outgoingConfirmedHeightPrefix)+20)
	lower = append(lower, outgoingConfirmedHeightPrefix...)
	lower = appendUint64Fixed20(lower, height+1)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(outgoingConfirmedHeightPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte{}, iter.Key()...)

		walletID, _, txid, actionIndex, err := parseOutgoingConfirmedHeightIndexKey(k)
		if err != nil {
			return err
		}

		ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
		v, closer, err := batch.Get(ooKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: unconfirm outgoing output get: %w", err)
		}
		var rec outgoingOutputRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unconfirm outgoing output decode: %w", err)
		}
		_ = closer.Close()

		rec.ConfirmedHeight = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: unconfirm outgoing output encode: %w", err)
		}
		if err := batch.Set(ooKey, b, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm outgoing output set: %w", err)
		}
		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unconfirm outgoing output delete index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: unconfirm outgoing outputs iter: %w", err)
	}
	return nil
}

func rollbackCommitments(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: commitmentPrefix,
		UpperBound: prefixUpperBound(commitmentPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		if errors.Is(iter.Error(), pebble.ErrNotFound) || iter.Error() == nil {
			if err := batch.Set(keyMeta("next_commitment_pos"), uint64To8(0), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: set next_commitment_pos: %w", err)
			}
			return nil
		}
		return fmt.Errorf("rocksdb: commitments last: %w", iter.Error())
	}

	var lastKeepPos uint64
	found := false
	for iter.Valid() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		if uint64(rec.Height) <= height {
			lastKeepPos = rec.Position
			found = true
			break
		}
		iter.Prev()
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: commitments scan: %w", err)
	}

	var nextPos uint64 = 0
	if found {
		nextPos = lastKeepPos + 1
		lower := make([]byte, 0, len(commitmentPrefix)+20)
		lower = append(lower, commitmentPrefix...)
		lower = appendUint64Fixed20(lower, nextPos)
		if err := batch.DeleteRange(lower, prefixUpperBound(commitmentPrefix), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete commitments range: %w", err)
		}
	} else {
		if err := batch.DeleteRange(commitmentPrefix, prefixUpperBound(commitmentPrefix), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete commitments: %w", err)
		}
		nextPos = 0
	}

	if err := batch.Set(keyMeta("next_commitment_pos"), uint64To8(nextPos), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set next_commitment_pos: %w", err)
	}
	return nil
}

func keyEventPrefix(walletID string) []byte {
	b := make([]byte, 0, len(eventPrefix)+len(walletID)+1)
	b = append(b, eventPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	return b
}

func keyEventHeightPrefix(height uint64, walletID string) []byte {
	b := make([]byte, 0, len(eventHeightPrefix)+20+1+len(walletID)+1)
	b = append(b, eventHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	b = append(b, walletID...)
	b = append(b, '/')
	return b
}

func parseEventID(key []byte, walletID string) (uint64, error) {
	prefix := keyEventPrefix(walletID)
	if !bytes.HasPrefix(key, prefix) {
		return 0, errors.New("rocksdb: event key prefix mismatch")
	}
	idBytes := bytes.TrimPrefix(key, prefix)
	if len(idBytes) != 20 {
		return 0, errors.New("rocksdb: event id invalid")
	}
	return strconv.ParseUint(string(idBytes), 10, 64)
}

func parseEventHeightIndexID(key []byte, height uint64, walletID string) (uint64, error) {
	prefix := keyEventHeightPrefix(height, walletID)
	if !bytes.HasPrefix(key, prefix) {
		return 0, errors.New("rocksdb: event height key prefix mismatch")
	}
	idBytes := bytes.TrimPrefix(key, prefix)
	if len(idBytes) != 20 {
		return 0, errors.New("rocksdb: event height id invalid")
	}
	return strconv.ParseUint(string(idBytes), 10, 64)
}

func noteKeyFromWalletNoteIndex(key []byte, walletID string) ([]byte, error) {
	prefix := keyWalletNotePrefix(walletID)
	if !bytes.HasPrefix(key, prefix) {
		return nil, errors.New("rocksdb: wallet note index prefix mismatch")
	}
	rest := bytes.TrimPrefix(key, prefix) // <height>/<txid>/<action_index>
	parts := bytes.Split(rest, []byte("/"))
	if len(parts) != 3 {
		return nil, errors.New("rocksdb: wallet note index malformed")
	}
	actionIndex, err := strconv.ParseInt(string(parts[2]), 10, 32)
	if err != nil {
		return nil, errors.New("rocksdb: wallet note action_index invalid")
	}
	return keyNote(walletID, string(parts[1]), int32(actionIndex)), nil
}

func parseNoteHeightIndexKey(key []byte) (walletID string, height int64, txid string, actionIndex int32, err error) {
	// k = nh/<height>/<wallet>/<txid>/<action_index>
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 5 {
		return "", 0, "", 0, errors.New("rocksdb: note height index malformed")
	}
	h, err := parseFixed20Int64(parts[1])
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: note height invalid")
	}
	action, err := strconv.ParseInt(string(parts[4]), 10, 32)
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: note action_index invalid")
	}
	return string(parts[2]), h, string(parts[3]), int32(action), nil
}

func parseOutgoingMinedHeightIndexKey(key []byte) (walletID string, height int64, txid string, actionIndex int32, err error) {
	// k = oomh/<height>/<wallet>/<txid>/<action_index>
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 5 {
		return "", 0, "", 0, errors.New("rocksdb: outgoing mined height index malformed")
	}
	h, err := parseFixed20Int64(parts[1])
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: outgoing mined height invalid")
	}
	action, err := strconv.ParseInt(string(parts[4]), 10, 32)
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: outgoing action_index invalid")
	}
	return string(parts[2]), h, string(parts[3]), int32(action), nil
}

func parseOutgoingConfirmedHeightIndexKey(key []byte) (walletID string, height int64, txid string, actionIndex int32, err error) {
	// k = ooch/<height>/<wallet>/<txid>/<action_index>
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 5 {
		return "", 0, "", 0, errors.New("rocksdb: outgoing confirmed height index malformed")
	}
	h, err := parseFixed20Int64(parts[1])
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: outgoing confirmed height invalid")
	}
	action, err := strconv.ParseInt(string(parts[4]), 10, 32)
	if err != nil {
		return "", 0, "", 0, errors.New("rocksdb: outgoing action_index invalid")
	}
	return string(parts[2]), h, string(parts[3]), int32(action), nil
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

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
