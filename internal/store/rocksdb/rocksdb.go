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
	"sort"
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
	v, closer, err := s.db.Get(verKey)
	if err == nil {
		version := string(v)
		_ = closer.Close()
		switch version {
		case "5":
			return s.ensureV4IdentityAndProgress()
		case "4":
			if err := s.migrateV4ToV5(verKey); err != nil {
				return err
			}
			return s.ensureV4IdentityAndProgress()
		case "3":
			if err := s.migrateV3ToV4(verKey); err != nil {
				return err
			}
			if err := s.migrateV4ToV5(verKey); err != nil {
				return err
			}
			return s.ensureV4IdentityAndProgress()
		case "2":
			if err := s.migrateV2ToV3(verKey); err != nil {
				return err
			}
			if err := s.migrateV3ToV4(verKey); err != nil {
				return err
			}
			if err := s.migrateV4ToV5(verKey); err != nil {
				return err
			}
			return s.ensureV4IdentityAndProgress()
		case "1":
			if err := s.migrateV1ToV2(verKey); err != nil {
				return err
			}
			if err := s.migrateV2ToV3(verKey); err != nil {
				return err
			}
			if err := s.migrateV3ToV4(verKey); err != nil {
				return err
			}
			if err := s.migrateV4ToV5(verKey); err != nil {
				return err
			}
			return s.ensureV4IdentityAndProgress()
		default:
			return fmt.Errorf("rocksdb: unsupported schema_version %q", version)
		}
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: schema_version: %w", err)
	}

	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(verKey, []byte("5"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set schema_version: %w", err)
	}
	epoch, err := store.NewEventEpoch()
	if err != nil {
		return err
	}
	if err := b.Set(keyMeta("event_epoch"), []byte(epoch), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set event epoch: %w", err)
	}
	if err := b.Set(keyMeta("outgoing_expiry_observation_repaired"), []byte("1"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set outgoing expiry repair marker: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate commit: %w", err)
	}
	return nil
}

func (s *Store) ensureV4IdentityAndProgress() error {
	b := s.db.NewBatch()
	defer b.Close()
	owners := make(map[string]string)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletPrefix, UpperBound: prefixUpperBound(walletPrefix)})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		walletID := strings.TrimPrefix(string(iter.Key()), string(walletPrefix))
		var wallet walletRecord
		if err := json.Unmarshal(iter.Value(), &wallet); err != nil {
			_ = iter.Close()
			return fmt.Errorf("rocksdb: audit wallet %q: %w", walletID, err)
		}
		ufvk := strings.TrimSpace(wallet.UFVK)
		if walletID == "" || ufvk == "" {
			_ = iter.Close()
			return fmt.Errorf("rocksdb: invalid wallet identity %q", walletID)
		}
		if owner, exists := owners[ufvk]; exists && owner != walletID {
			_ = iter.Close()
			return fmt.Errorf("rocksdb: duplicate ufvk for %q and %q: %w", owner, walletID, store.ErrUFVKAlreadyRegistered)
		}
		owners[ufvk] = walletID
		stateBytes, stateCloser, stateErr := s.db.Get(keyWalletUnspentState(walletID))
		if stateErr != nil {
			_ = iter.Close()
			if errors.Is(stateErr, pebble.ErrNotFound) {
				return store.ErrInvalidWalletNoteState
			}
			return fmt.Errorf("rocksdb: audit wallet unspent state %q: %w", walletID, stateErr)
		}
		if len(stateBytes) != 8 || binary.BigEndian.Uint64(stateBytes) > uint64(^uint64(0)>>1) {
			_ = stateCloser.Close()
			_ = iter.Close()
			return store.ErrInvalidWalletNoteState
		}
		_ = stateCloser.Close()

		progressKey := keyWalletBackfill(walletID)
		progress := walletBackfillRecord{BirthdayHeight: wallet.BirthdayHeight, NextHeight: wallet.BirthdayHeight, State: "pending", Generation: 1, UpdatedAtUnix: time.Now().UTC().Unix()}
		v, closer, getErr := s.db.Get(progressKey)
		if getErr == nil {
			if err := json.Unmarshal(v, &progress); err != nil {
				_ = closer.Close()
				_ = iter.Close()
				return fmt.Errorf("rocksdb: audit wallet progress %q: %w", walletID, err)
			}
			_ = closer.Close()
		} else if !errors.Is(getErr, pebble.ErrNotFound) {
			_ = iter.Close()
			return getErr
		}
		changed := getErr != nil
		if progress.Generation < 1 {
			progress.Generation = 1
			changed = true
		}
		if progress.BirthdayHeight != wallet.BirthdayHeight {
			progress.BirthdayHeight = wallet.BirthdayHeight
			progress.NextHeight = wallet.BirthdayHeight
			progress.TargetHeight = 0
			progress.State = "pending"
			progress.LastError = ""
			progress.Generation++
			progress.UpdatedAtUnix = time.Now().UTC().Unix()
			changed = true
		}
		if changed {
			encoded, err := json.Marshal(progress)
			if err != nil {
				_ = iter.Close()
				return err
			}
			if err := b.Set(progressKey, encoded, pebble.NoSync); err != nil {
				_ = iter.Close()
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return err
	}
	_ = iter.Close()
	if err := s.repairLegacyOutgoingExpiryObservations(b); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

func (s *Store) repairLegacyOutgoingExpiryObservations(batch *pebble.Batch) error {
	marker := keyMeta("outgoing_expiry_observation_repaired")
	if _, closer, err := s.db.Get(marker); err == nil {
		_ = closer.Close()
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: outgoing expiry repair marker: %w", err)
	}

	type expiryKey struct {
		walletID    string
		txid        string
		actionIndex int32
	}
	expiryHeights := make(map[expiryKey]int64)
	eventsIter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: eventPrefix, UpperBound: prefixUpperBound(eventPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: legacy expiry events iter: %w", err)
	}
	for eventsIter.First(); eventsIter.Valid(); eventsIter.Next() {
		var rec eventRecord
		if err := json.Unmarshal(eventsIter.Value(), &rec); err != nil {
			_ = eventsIter.Close()
			return fmt.Errorf("rocksdb: legacy expiry event decode: %w", err)
		}
		if rec.Kind != events.KindOutgoingOutputExpired {
			continue
		}
		var payload struct {
			TxID        string `json:"txid"`
			ActionIndex *int32 `json:"action_index"`
		}
		if err := json.Unmarshal([]byte(rec.Payload), &payload); err != nil || payload.ActionIndex == nil {
			continue
		}
		key := expiryKey{walletID: rec.WalletID, txid: strings.ToLower(strings.TrimSpace(payload.TxID)), actionIndex: *payload.ActionIndex}
		if rec.Height > expiryHeights[key] {
			expiryHeights[key] = rec.Height
		}
	}
	if err := eventsIter.Error(); err != nil {
		_ = eventsIter.Close()
		return fmt.Errorf("rocksdb: legacy expiry events iter: %w", err)
	}
	_ = eventsIter.Close()

	outputs, err := s.db.NewIter(&pebble.IterOptions{LowerBound: outgoingOutputPrefix, UpperBound: prefixUpperBound(outgoingOutputPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: legacy expiry outputs iter: %w", err)
	}
	for outputs.First(); outputs.Valid(); outputs.Next() {
		var rec outgoingOutputRecord
		if err := json.Unmarshal(outputs.Value(), &rec); err != nil {
			_ = outputs.Close()
			return fmt.Errorf("rocksdb: legacy expiry output decode: %w", err)
		}
		if rec.ExpiredAtUnix == nil || rec.ExpiredHeight != nil {
			continue
		}
		height, ok := expiryHeights[expiryKey{walletID: rec.WalletID, txid: strings.ToLower(strings.TrimSpace(rec.TxID)), actionIndex: rec.ActionIndex}]
		if !ok {
			height = int64(1<<63 - 1)
		}
		rec.ExpiredHeight = &height
		encoded, err := json.Marshal(rec)
		if err != nil {
			_ = outputs.Close()
			return err
		}
		if err := batch.Set(append([]byte{}, outputs.Key()...), encoded, pebble.NoSync); err != nil {
			_ = outputs.Close()
			return err
		}
	}
	if err := outputs.Error(); err != nil {
		_ = outputs.Close()
		return fmt.Errorf("rocksdb: legacy expiry outputs iter: %w", err)
	}
	_ = outputs.Close()
	if err := batch.Set(marker, []byte("1"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set outgoing expiry repair marker: %w", err)
	}
	return nil
}

func (s *Store) migrateV4ToV5(verKey []byte) error {
	b := s.db.NewBatch()
	defer b.Close()

	if err := b.DeleteRange(walletUnspentNotePrefix, prefixUpperBound(walletUnspentNotePrefix), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v5 clear unspent note index: %w", err)
	}
	if err := b.DeleteRange(walletUnspentStatePrefix, prefixUpperBound(walletUnspentStatePrefix), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v5 clear unspent state: %w", err)
	}

	counts := make(map[string]int64)
	wallets, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletPrefix, UpperBound: prefixUpperBound(walletPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v5 wallets: %w", err)
	}
	for wallets.First(); wallets.Valid(); wallets.Next() {
		walletID := strings.TrimPrefix(string(wallets.Key()), string(walletPrefix))
		if walletID == "" {
			_ = wallets.Close()
			return store.ErrInvalidWalletNoteState
		}
		counts[walletID] = 0
	}
	if err := wallets.Error(); err != nil {
		_ = wallets.Close()
		return fmt.Errorf("rocksdb: migrate v5 wallets: %w", err)
	}
	_ = wallets.Close()

	notes, err := s.db.NewIter(&pebble.IterOptions{LowerBound: notePrefix, UpperBound: prefixUpperBound(notePrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v5 notes: %w", err)
	}
	for notes.First(); notes.Valid(); notes.Next() {
		var rec noteRecord
		if err := json.Unmarshal(notes.Value(), &rec); err != nil {
			_ = notes.Close()
			return fmt.Errorf("rocksdb: migrate v5 decode note: %w", err)
		}
		if _, ok := counts[rec.WalletID]; !ok || rec.Height < 0 || rec.ActionIndex < 0 || !bytes.Equal(notes.Key(), keyNote(rec.WalletID, rec.TxID, rec.ActionIndex)) {
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		_, closer, err := s.db.Get(keyWalletNoteIndex(uint64(rec.Height), rec.WalletID, rec.TxID, rec.ActionIndex))
		if err != nil {
			_ = notes.Close()
			if errors.Is(err, pebble.ErrNotFound) {
				return store.ErrInvalidWalletNoteState
			}
			return fmt.Errorf("rocksdb: migrate v5 wallet note index: %w", err)
		}
		_ = closer.Close()
		if strings.TrimSpace(rec.NoteNullifier) == "" || rec.ValueZat < 0 ||
			(rec.Position != nil && *rec.Position > uint64(^uint32(0))) ||
			(rec.PendingSpentExpiryHeight != nil && *rec.PendingSpentExpiryHeight < 0) ||
			(rec.PendingSpentTxID == nil && (rec.PendingSpentAtUnix != nil || rec.PendingSpentExpiryHeight != nil)) ||
			(rec.PendingSpentTxID != nil && rec.PendingSpentAtUnix == nil) {
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		heightIndexValue, heightCloser, err := s.db.Get(keyNoteHeightIndex(uint64(rec.Height), rec.WalletID, rec.TxID, rec.ActionIndex))
		if err != nil {
			_ = notes.Close()
			if errors.Is(err, pebble.ErrNotFound) {
				return store.ErrInvalidWalletNoteState
			}
			return fmt.Errorf("rocksdb: migrate v5 note height index: %w", err)
		}
		if string(heightIndexValue) != rec.NoteNullifier {
			_ = heightCloser.Close()
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		_ = heightCloser.Close()
		nullifierValue, nullifierCloser, err := s.db.Get(keyNullifier(rec.NoteNullifier))
		if err != nil {
			_ = notes.Close()
			if errors.Is(err, pebble.ErrNotFound) {
				return store.ErrInvalidWalletNoteState
			}
			return fmt.Errorf("rocksdb: migrate v5 nullifier index: %w", err)
		}
		if !bytes.Equal(nullifierValue, notes.Key()) {
			_ = nullifierCloser.Close()
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		_ = nullifierCloser.Close()
		if rec.SpentHeight != nil {
			if *rec.SpentHeight < 0 || rec.SpentTxID == nil || rec.PendingSpentTxID != nil || rec.PendingSpentAtUnix != nil || rec.PendingSpentExpiryHeight != nil {
				_ = notes.Close()
				return store.ErrInvalidWalletNoteState
			}
			continue
		}
		if rec.SpentTxID != nil || rec.SpentConfirmedHeight != nil {
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		if counts[rec.WalletID] == int64(^uint64(0)>>1) {
			_ = notes.Close()
			return store.ErrInvalidWalletNoteState
		}
		counts[rec.WalletID]++
		noteKey := append([]byte{}, notes.Key()...)
		if err := b.Set(keyWalletUnspentNote(rec.WalletID, rec.TxID, rec.ActionIndex), noteKey, pebble.NoSync); err != nil {
			_ = notes.Close()
			return fmt.Errorf("rocksdb: migrate v5 unspent note index: %w", err)
		}
	}
	if err := notes.Error(); err != nil {
		_ = notes.Close()
		return fmt.Errorf("rocksdb: migrate v5 notes: %w", err)
	}
	_ = notes.Close()

	for walletID, count := range counts {
		if err := b.Set(keyWalletUnspentState(walletID), uint64To8(uint64(count)), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: migrate v5 unspent state: %w", err)
		}
	}
	if err := b.Set(verKey, []byte("5"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v5 version: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v5 commit: %w", err)
	}
	return nil
}

func (s *Store) migrateV3ToV4(verKey []byte) error {
	b := s.db.NewBatch()
	defer b.Close()

	wallets := make(map[string]walletRecord)
	ufvkOwners := make(map[string]string)
	walletIter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletPrefix, UpperBound: prefixUpperBound(walletPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v4 wallets: %w", err)
	}
	for walletIter.First(); walletIter.Valid(); walletIter.Next() {
		walletID := strings.TrimPrefix(string(walletIter.Key()), string(walletPrefix))
		var rec walletRecord
		if err := json.Unmarshal(walletIter.Value(), &rec); err != nil {
			_ = walletIter.Close()
			return fmt.Errorf("rocksdb: migrate v4 decode wallet %q: %w", walletID, err)
		}
		ufvk := strings.TrimSpace(rec.UFVK)
		if walletID == "" || ufvk == "" {
			_ = walletIter.Close()
			return fmt.Errorf("rocksdb: migrate v4 invalid wallet identity %q", walletID)
		}
		if owner, exists := ufvkOwners[ufvk]; exists && owner != walletID {
			_ = walletIter.Close()
			return fmt.Errorf("rocksdb: migrate v4 duplicate ufvk for %q and %q: %w", owner, walletID, store.ErrUFVKAlreadyRegistered)
		}
		ufvkOwners[ufvk] = walletID
		wallets[walletID] = rec
	}
	if err := walletIter.Error(); err != nil {
		_ = walletIter.Close()
		return err
	}
	_ = walletIter.Close()

	notes, err := s.db.NewIter(&pebble.IterOptions{LowerBound: notePrefix, UpperBound: prefixUpperBound(notePrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v4 notes: %w", err)
	}
	for notes.First(); notes.Valid(); notes.Next() {
		var rec noteRecord
		if err := json.Unmarshal(notes.Value(), &rec); err != nil {
			_ = notes.Close()
			return fmt.Errorf("rocksdb: migrate v4 decode note: %w", err)
		}
		eligible := false
		rec.DepositEligible = &eligible
		rec.ConfirmedHeight = nil
		encoded, err := json.Marshal(rec)
		if err != nil {
			_ = notes.Close()
			return err
		}
		if err := b.Set(append([]byte{}, notes.Key()...), encoded, pebble.NoSync); err != nil {
			_ = notes.Close()
			return err
		}
	}
	if err := notes.Error(); err != nil {
		_ = notes.Close()
		return err
	}
	_ = notes.Close()

	confirmed, err := s.db.NewIter(&pebble.IterOptions{LowerBound: noteConfirmedHeightPrefix, UpperBound: prefixUpperBound(noteConfirmedHeightPrefix)})
	if err != nil {
		return err
	}
	for confirmed.First(); confirmed.Valid(); confirmed.Next() {
		if err := b.Delete(append([]byte{}, confirmed.Key()...), pebble.NoSync); err != nil {
			_ = confirmed.Close()
			return err
		}
	}
	_ = confirmed.Close()

	eventIter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: eventPrefix, UpperBound: prefixUpperBound(eventPrefix)})
	if err != nil {
		return err
	}
	for eventIter.First(); eventIter.Valid(); eventIter.Next() {
		var rec eventRecord
		if err := json.Unmarshal(eventIter.Value(), &rec); err != nil {
			_ = eventIter.Close()
			return fmt.Errorf("rocksdb: migrate v4 decode event: %w", err)
		}
		if !isDepositLifecycleKind(rec.Kind) {
			continue
		}
		id, err := eventIDFromKey(eventIter.Key())
		if err != nil {
			_ = eventIter.Close()
			return err
		}
		if err := b.Delete(append([]byte{}, eventIter.Key()...), pebble.NoSync); err != nil {
			_ = eventIter.Close()
			return err
		}
		if err := b.Delete(keyEventHeightIndex(uint64(rec.Height), rec.WalletID, id), pebble.NoSync); err != nil {
			_ = eventIter.Close()
			return err
		}
	}
	_ = eventIter.Close()

	progressIter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletBackfillPrefix, UpperBound: prefixUpperBound(walletBackfillPrefix)})
	if err != nil {
		return err
	}
	progressIDs := make(map[string]struct{})
	for progressIter.First(); progressIter.Valid(); progressIter.Next() {
		walletID := strings.TrimPrefix(string(progressIter.Key()), string(walletBackfillPrefix))
		progressIDs[walletID] = struct{}{}
		var rec walletBackfillRecord
		if err := json.Unmarshal(progressIter.Value(), &rec); err != nil {
			_ = progressIter.Close()
			return err
		}
		rec.NextHeight, rec.State, rec.LastError = rec.BirthdayHeight, "pending", ""
		if rec.Generation < 1 {
			rec.Generation = 1
		}
		rec.UpdatedAtUnix = time.Now().UTC().Unix()
		encoded, err := json.Marshal(rec)
		if err != nil {
			_ = progressIter.Close()
			return err
		}
		if err := b.Set(append([]byte{}, progressIter.Key()...), encoded, pebble.NoSync); err != nil {
			_ = progressIter.Close()
			return err
		}
	}
	_ = progressIter.Close()
	for walletID, wallet := range wallets {
		if _, exists := progressIDs[walletID]; exists {
			continue
		}
		rec := walletBackfillRecord{BirthdayHeight: wallet.BirthdayHeight, NextHeight: wallet.BirthdayHeight, State: "pending", Generation: 1, UpdatedAtUnix: time.Now().UTC().Unix()}
		encoded, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := b.Set(keyWalletBackfill(walletID), encoded, pebble.NoSync); err != nil {
			return err
		}
	}

	epoch, err := store.NewEventEpoch()
	if err != nil {
		return err
	}
	if err := b.Set(keyMeta("event_epoch"), []byte(epoch), pebble.NoSync); err != nil {
		return err
	}
	if err := b.Set(verKey, []byte("4"), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

func (s *Store) EventEpoch(ctx context.Context) (string, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	v, closer, err := s.db.Get(keyMeta("event_epoch"))
	if err == nil {
		epoch := string(v)
		_ = closer.Close()
		return epoch, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return "", fmt.Errorf("rocksdb: event epoch: %w", err)
	}
	epoch, err := store.NewEventEpoch()
	if err != nil {
		return "", err
	}
	if err := s.db.Set(keyMeta("event_epoch"), []byte(epoch), pebble.NoSync); err != nil {
		return "", fmt.Errorf("rocksdb: initialize event epoch: %w", err)
	}
	return epoch, nil
}

func (s *Store) RotateEventEpoch(ctx context.Context) (string, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	epoch, err := store.NewEventEpoch()
	if err != nil {
		return "", err
	}
	if err := s.db.Set(keyMeta("event_epoch"), []byte(epoch), pebble.NoSync); err != nil {
		return "", fmt.Errorf("rocksdb: rotate event epoch: %w", err)
	}
	return epoch, nil
}

func (s *Store) migrateV2ToV3(verKey []byte) error {
	b := s.db.NewBatch()
	defer b.Close()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: notePrefix,
		UpperBound: prefixUpperBound(notePrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v3 iter: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var rec noteRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: migrate v3 decode note: %w", err)
		}
		noteKey := append([]byte{}, iter.Key()...)
		if err := b.Set(keyWalletAddressNoteIndex(uint64(rec.Height), rec.WalletID, rec.RecipientAddress, rec.TxID, rec.ActionIndex), noteKey, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: migrate v3 address note index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: migrate v3 iter: %w", err)
	}
	if err := b.Set(verKey, []byte("3"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v3 version: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v3 commit: %w", err)
	}
	return nil
}

func (s *Store) migrateV1ToV2(verKey []byte) error {
	b := s.db.NewBatch()
	defer b.Close()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: outgoingOutputPrefix,
		UpperBound: prefixUpperBound(outgoingOutputPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: migrate v2 iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec outgoingOutputRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: migrate v2 decode outgoing output: %w", err)
		}
		if rec.MinedHeight == nil {
			continue
		}
		if err := b.Set(keyWalletOutgoingMinedIndex(rec.WalletID, uint64(*rec.MinedHeight), rec.TxID, rec.ActionIndex), nil, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: migrate v2 wallet outgoing index: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: migrate v2 iter: %w", err)
	}

	if err := b.Set(verKey, []byte("2"), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v2 version: %w", err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: migrate v2 commit: %w", err)
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
	walletExists := err == nil
	if err == nil {
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: decode wallet: %w", err)
		}
		_ = closer.Close()
		if rec.UFVK != ufvk {
			return store.ErrWalletUFVKMismatch
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get wallet: %w", err)
	} else {
		rec.CreatedAtUnix = now.Unix()
	}
	if err := s.rejectDuplicateUFVKLocked(walletID, ufvk); err != nil {
		return err
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
	if walletExists {
		if _, closer, err := s.db.Get(keyWalletUnspentState(walletID)); err == nil {
			_ = closer.Close()
		} else if errors.Is(err, pebble.ErrNotFound) {
			return store.ErrInvalidWalletNoteState
		} else {
			return fmt.Errorf("rocksdb: get wallet unspent state: %w", err)
		}
	} else if err := batch.Set(keyWalletUnspentState(walletID), uint64To8(0), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: initialize wallet unspent state: %w", err)
	}
	progressKey := keyWalletBackfill(walletID)
	if value, closer, err := s.db.Get(progressKey); errors.Is(err, pebble.ErrNotFound) {
		progress, marshalErr := json.Marshal(walletBackfillRecord{BirthdayHeight: rec.BirthdayHeight, NextHeight: rec.BirthdayHeight, State: "pending", Generation: 1, UpdatedAtUnix: now.Unix()})
		if marshalErr != nil {
			return marshalErr
		}
		if err := batch.Set(progressKey, progress, pebble.NoSync); err != nil {
			return err
		}
	} else if err == nil {
		var progress walletBackfillRecord
		if err := json.Unmarshal(value, &progress); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: decode wallet backfill: %w", err)
		}
		_ = closer.Close()
		changed := false
		if progress.Generation < 1 {
			progress.Generation = 1
			changed = true
		}
		if progress.BirthdayHeight != rec.BirthdayHeight {
			progress.BirthdayHeight = rec.BirthdayHeight
			progress.NextHeight = rec.BirthdayHeight
			progress.TargetHeight = 0
			progress.State = "pending"
			progress.LastError = ""
			progress.Generation++
			changed = true
		}
		if changed {
			progress.UpdatedAtUnix = now.Unix()
			encoded, err := json.Marshal(progress)
			if err != nil {
				return err
			}
			if err := batch.Set(progressKey, encoded, pebble.NoSync); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("rocksdb: get wallet backfill: %w", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: upsert wallet commit: %w", err)
	}
	return nil
}

func (s *Store) UpsertWalletBirthday(ctx context.Context, walletID, ufvk string, birthdayHeight int64) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	key := keyWallet(walletID)
	now := time.Now().UTC()
	var rec walletRecord
	v, closer, err := s.db.Get(key)
	walletExists := err == nil
	if err == nil {
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: decode wallet: %w", err)
		}
		_ = closer.Close()
		if rec.UFVK != ufvk {
			return store.ErrWalletUFVKMismatch
		}
		if birthdayHeight > rec.BirthdayHeight {
			return store.ErrBirthdayIncrease
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get wallet: %w", err)
	} else {
		rec.CreatedAtUnix = now.Unix()
	}
	if err := s.rejectDuplicateUFVKLocked(walletID, ufvk); err != nil {
		return err
	}
	oldBirthday := rec.BirthdayHeight
	rec.UFVK, rec.BirthdayHeight, rec.DisabledAtUnix = ufvk, birthdayHeight, nil
	walletBytes, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode wallet: %w", err)
	}
	progress := walletBackfillRecord{BirthdayHeight: birthdayHeight, NextHeight: birthdayHeight, State: "pending", Generation: 1, UpdatedAtUnix: now.Unix()}
	if v, closer, err := s.db.Get(keyWalletBackfill(walletID)); err == nil {
		if err := json.Unmarshal(v, &progress); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: decode wallet backfill: %w", err)
		}
		_ = closer.Close()
		if oldBirthday != birthdayHeight || progress.BirthdayHeight != birthdayHeight {
			progress.NextHeight, progress.State = birthdayHeight, "pending"
			progress.TargetHeight = 0
			progress.LastError = ""
			progress.Generation++
		}
		if progress.Generation < 1 {
			progress.Generation = 1
		}
		progress.BirthdayHeight = birthdayHeight
		progress.UpdatedAtUnix = now.Unix()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("rocksdb: get wallet backfill: %w", err)
	}
	progressBytes, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("rocksdb: encode wallet backfill: %w", err)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(key, walletBytes, pebble.NoSync); err != nil {
		return err
	}
	if walletExists {
		if _, closer, err := s.db.Get(keyWalletUnspentState(walletID)); err == nil {
			_ = closer.Close()
		} else if errors.Is(err, pebble.ErrNotFound) {
			return store.ErrInvalidWalletNoteState
		} else {
			return fmt.Errorf("rocksdb: get wallet unspent state: %w", err)
		}
	} else if err := batch.Set(keyWalletUnspentState(walletID), uint64To8(0), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: initialize wallet unspent state: %w", err)
	}
	if err := batch.Set(keyWalletBackfill(walletID), progressBytes, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) rejectDuplicateUFVKLocked(walletID, ufvk string) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletPrefix, UpperBound: prefixUpperBound(walletPrefix)})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		otherID := strings.TrimPrefix(string(iter.Key()), string(walletPrefix))
		if otherID == walletID {
			continue
		}
		var rec walletRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return err
		}
		if rec.UFVK == ufvk {
			return store.ErrUFVKAlreadyRegistered
		}
	}
	return iter.Error()
}

func (s *Store) WalletBackfillStatus(ctx context.Context, walletID string) (store.WalletBackfillProgress, bool, error) {
	_ = ctx
	p := store.WalletBackfillProgress{WalletID: walletID}
	v, closer, err := s.db.Get(keyWalletBackfill(walletID))
	if errors.Is(err, pebble.ErrNotFound) {
		return p, false, nil
	}
	if err != nil {
		return p, false, fmt.Errorf("rocksdb: wallet backfill status: %w", err)
	}
	defer closer.Close()
	var rec walletBackfillRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return p, false, fmt.Errorf("rocksdb: decode wallet backfill: %w", err)
	}
	p.BirthdayHeight, p.NextHeight, p.TargetHeight, p.State, p.LastError, p.Generation = rec.BirthdayHeight, rec.NextHeight, rec.TargetHeight, rec.State, rec.LastError, rec.Generation
	p.UpdatedAt = time.Unix(rec.UpdatedAtUnix, 0).UTC()
	walletBytes, walletCloser, walletErr := s.db.Get(keyWallet(walletID))
	if walletErr != nil {
		return p, false, fmt.Errorf("rocksdb: wallet backfill identity: %w", walletErr)
	}
	var wallet walletRecord
	if err := json.Unmarshal(walletBytes, &wallet); err != nil {
		_ = walletCloser.Close()
		return p, false, fmt.Errorf("rocksdb: decode wallet identity: %w", err)
	}
	_ = walletCloser.Close()
	p.UFVKFingerprint = store.UFVKFingerprint(wallet.UFVK)
	return p, true, nil
}

func (s *Store) SetWalletBackfillProgress(ctx context.Context, p store.WalletBackfillProgress) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	v, closer, err := s.db.Get(keyWalletBackfill(p.WalletID))
	if errors.Is(err, pebble.ErrNotFound) {
		return store.ErrBackfillProgressConflict
	}
	if err != nil {
		return fmt.Errorf("rocksdb: get wallet backfill for update: %w", err)
	}
	var current walletBackfillRecord
	if err := json.Unmarshal(v, &current); err != nil {
		_ = closer.Close()
		return fmt.Errorf("rocksdb: decode wallet backfill for update: %w", err)
	}
	_ = closer.Close()
	if p.Generation < 1 || current.Generation != p.Generation || current.BirthdayHeight != p.BirthdayHeight || (p.ExpectedNextHeight != nil && current.NextHeight != *p.ExpectedNextHeight) {
		return store.ErrBackfillProgressConflict
	}
	rec := current
	rec.NextHeight, rec.TargetHeight, rec.State, rec.LastError, rec.UpdatedAtUnix = p.NextHeight, p.TargetHeight, p.State, p.LastError, time.Now().UTC().Unix()
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode wallet backfill: %w", err)
	}
	if err := s.db.Set(keyWalletBackfill(p.WalletID), b, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set wallet backfill: %w", err)
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
			WalletID:        walletID,
			UFVKFingerprint: store.UFVKFingerprint(rec.UFVK),
			BirthdayHeight:  rec.BirthdayHeight,
			CreatedAt:       time.Unix(rec.CreatedAtUnix, 0).UTC(),
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
		type preservedKV struct{ key, value []byte }
		preserved := make([]preservedKV, 0)
		epoch, err := store.NewEventEpoch()
		if err != nil {
			return err
		}
		wallets, err := s.db.NewIter(&pebble.IterOptions{LowerBound: walletPrefix, UpperBound: prefixUpperBound(walletPrefix)})
		if err != nil {
			return fmt.Errorf("rocksdb: rollback list wallets: %w", err)
		}
		for wallets.First(); wallets.Valid(); wallets.Next() {
			walletID := strings.TrimPrefix(string(wallets.Key()), string(walletPrefix))
			var wallet walletRecord
			if err := json.Unmarshal(wallets.Value(), &wallet); err != nil {
				_ = wallets.Close()
				return fmt.Errorf("rocksdb: rollback decode wallet %q: %w", walletID, err)
			}
			preserved = append(preserved, preservedKV{append([]byte{}, wallets.Key()...), append([]byte{}, wallets.Value()...)})
			preserved = append(preserved, preservedKV{keyWalletUnspentState(walletID), uint64To8(0)})
			generation := int64(1)
			if v, closer, err := s.db.Get(keyWalletBackfill(walletID)); err == nil {
				var progress walletBackfillRecord
				if err := json.Unmarshal(v, &progress); err != nil {
					_ = closer.Close()
					_ = wallets.Close()
					return fmt.Errorf("rocksdb: rollback decode wallet progress %q: %w", walletID, err)
				}
				_ = closer.Close()
				if progress.Generation >= generation {
					generation = progress.Generation + 1
				}
			} else if !errors.Is(err, pebble.ErrNotFound) {
				_ = wallets.Close()
				return fmt.Errorf("rocksdb: rollback read wallet progress %q: %w", walletID, err)
			}
			progress := walletBackfillRecord{BirthdayHeight: wallet.BirthdayHeight, NextHeight: wallet.BirthdayHeight, State: "pending", Generation: generation, UpdatedAtUnix: time.Now().UTC().Unix()}
			encoded, err := json.Marshal(progress)
			if err != nil {
				_ = wallets.Close()
				return err
			}
			preserved = append(preserved, preservedKV{keyWalletBackfill(walletID), encoded})
		}
		if err := wallets.Error(); err != nil {
			_ = wallets.Close()
			return err
		}
		_ = wallets.Close()
		for _, name := range []string{"schema_version"} {
			v, closer, err := s.db.Get(keyMeta(name))
			if err != nil {
				return fmt.Errorf("rocksdb: rollback preserve %s: %w", name, err)
			}
			preserved = append(preserved, preservedKV{keyMeta(name), append([]byte{}, v...)})
			_ = closer.Close()
		}
		preserved = append(preserved, preservedKV{keyMeta("event_epoch"), []byte(epoch)})
		b := s.db.NewBatch()
		defer b.Close()
		if err := b.DeleteRange([]byte{0x00}, []byte{0xFF}, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: rollback clear: %w", err)
		}
		for _, kv := range preserved {
			if err := b.Set(kv.key, kv.value, pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: rollback restore identity: %w", err)
			}
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
			IsInternal:           noteRecordIsInternal(rec),
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
		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		var expiredAt *time.Time
		if rec.ExpiredAtUnix != nil {
			t := time.Unix(*rec.ExpiredAtUnix, 0).UTC()
			expiredAt = &t
		}
		return store.OutgoingOutput{
			WalletID:         rec.WalletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      rec.MinedHeight,
			Position:         posPtr,
			ConfirmedHeight:  rec.ConfirmedHeight,
			MempoolSeenAt:    mempoolSeenAt,
			TxExpiryHeight:   rec.TxExpiryHeight,
			ExpiredAt:        expiredAt,
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
	if err := unexpireOutgoingOutputsAboveHeight(batch, s.db, height); err != nil {
		return err
	}
	_ = batch.Delete(keyMeta("confirmed_upto_outgoing_mined_height"), pebble.NoSync)

	// Delete commitments above height and fix next position.
	if err := rollbackCommitments(batch, s.db, uint64(height)); err != nil {
		return err
	}
	if err := rollbackSubtreeRoots(batch, s.db, uint64(height)); err != nil {
		return err
	}
	if err := rollbackShardRoots(batch, s.db, height); err != nil {
		return err
	}

	if height >= 0 {
		for _, n := range orphanDeposits {
			if n.IsInternal {
				continue
			}
			memoHex := ""
			if n.MemoHex != nil {
				memoHex = *n.MemoHex
			}
			payload := events.DepositOrphanedPayload{
				DepositEventPayload: events.DepositEventPayload{
					Origin: string(types.DepositOriginExternal),
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
			if n.IsInternal || n.ConfirmedHeight == nil {
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
					Origin: string(types.DepositOriginExternal),
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

func (s *Store) MaxWalletEventID(ctx context.Context, walletID string) (int64, error) {
	_ = ctx
	prefix := keyEvent(walletID, 0)
	prefix = prefix[:len(prefix)-20]
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("rocksdb: max wallet event id iterator: %w", err)
	}
	defer iter.Close()
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("rocksdb: max wallet event id: %w", err)
		}
		return 0, nil
	}
	id, err := eventIDFromKey(iter.Key())
	if err != nil {
		return 0, fmt.Errorf("rocksdb: max wallet event id: %w", err)
	}
	if id > uint64(^uint64(0)>>1) {
		return 0, errors.New("rocksdb: max wallet event id overflow")
	}
	return int64(id), nil
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

func sanitizeNotesQuery(query store.NotesQuery) store.NotesQuery {
	if query.Limit <= 0 || query.Limit > 1000 {
		query.Limit = 1000
	}
	if query.MinValueZat < 0 {
		query.MinValueZat = 0
	}
	if query.Cursor != nil {
		cursor := *query.Cursor
		cursor.TxID = strings.ToLower(strings.TrimSpace(cursor.TxID))
		query.Cursor = &cursor
		if query.Cursor.TxID == "" {
			query.Cursor = nil
		}
	}
	return query
}

func sanitizeOutgoingOutputsQuery(query store.OutgoingOutputsQuery) store.OutgoingOutputsQuery {
	if query.Limit <= 0 || query.Limit > 1000 {
		query.Limit = 1000
	}
	if query.MinValueZat < 0 {
		query.MinValueZat = 0
	}
	if query.Cursor != nil {
		cursor := *query.Cursor
		cursor.TxID = strings.ToLower(strings.TrimSpace(cursor.TxID))
		query.Cursor = &cursor
		if query.Cursor.TxID == "" {
			query.Cursor = nil
		}
	}
	return query
}

func (s *Store) ListWalletNotesPage(ctx context.Context, walletID string, query store.NotesQuery) ([]store.Note, *store.NotesCursor, error) {
	_ = ctx
	query = sanitizeNotesQuery(query)

	lowerBound := keyWalletNoteIndexMin(walletID)
	upperBound := prefixUpperBound(keyWalletNotePrefix(walletID))
	if query.RecipientAddress != "" {
		lowerBound = keyWalletAddressNoteIndexMin(walletID, query.RecipientAddress)
		upperBound = prefixUpperBound(keyWalletAddressNotePrefix(walletID, query.RecipientAddress))
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	if query.Cursor == nil {
		iter.First()
	} else {
		cursorKey := keyWalletNoteIndex(uint64(query.Cursor.Height), walletID, query.Cursor.TxID, query.Cursor.ActionIndex)
		if query.RecipientAddress != "" {
			cursorKey = keyWalletAddressNoteIndex(uint64(query.Cursor.Height), walletID, query.RecipientAddress, query.Cursor.TxID, query.Cursor.ActionIndex)
		}
		iter.SeekGE(cursorKey)
		if iter.Valid() && bytes.Equal(iter.Key(), cursorKey) {
			iter.Next()
		}
	}

	out := make([]store.Note, 0, query.Limit+1)
	for ; iter.Valid(); iter.Next() {
		var noteKey []byte
		if query.RecipientAddress != "" {
			noteKey = append([]byte{}, iter.Value()...)
			if len(noteKey) == 0 {
				return nil, nil, errors.New("rocksdb: address note index malformed")
			}
		} else {
			noteKey, err = noteKeyFromWalletNoteIndex(iter.Key(), walletID)
			if err != nil {
				return nil, nil, err
			}
		}

		v, closer, err := s.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, nil, fmt.Errorf("rocksdb: get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, nil, fmt.Errorf("rocksdb: decode note: %w", err)
		}
		_ = closer.Close()

		if query.OnlyUnspent && rec.SpentHeight != nil {
			continue
		}
		if query.MinValueZat > 0 && rec.ValueZat < query.MinValueZat {
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
			WalletID:                 walletID,
			TxID:                     rec.TxID,
			ActionIndex:              rec.ActionIndex,
			Height:                   rec.Height,
			Position:                 posPtr,
			DiversifierIndex:         rec.DiversifierIndex,
			RecipientAddress:         rec.RecipientAddress,
			ValueZat:                 rec.ValueZat,
			MemoHex:                  rec.MemoHex,
			NoteNullifier:            rec.NoteNullifier,
			PendingSpentTxID:         rec.PendingSpentTxID,
			PendingSpentAt:           pendingAt,
			PendingSpentExpiryHeight: rec.PendingSpentExpiryHeight,
			SpentHeight:              rec.SpentHeight,
			SpentTxID:                rec.SpentTxID,
			ConfirmedHeight:          rec.ConfirmedHeight,
			SpentConfirmedHeight:     rec.SpentConfirmedHeight,
			CreatedAt:                time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
		if len(out) >= query.Limit+1 {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, nil, fmt.Errorf("rocksdb: list notes: %w", err)
	}

	if len(out) <= query.Limit {
		return out, nil, nil
	}
	last := out[query.Limit-1]
	next := &store.NotesCursor{
		Height:      last.Height,
		TxID:        last.TxID,
		ActionIndex: last.ActionIndex,
	}
	return out[:query.Limit], next, nil
}

func (s *Store) WalletNoteSummary(ctx context.Context, walletID string, minConfirmations, minNoteZat int64, maxNotes int) (store.WalletNoteSummary, error) {
	var out store.WalletNoteSummary
	if minConfirmations < 0 || minNoteZat < 0 || maxNotes < 1 || maxNotes > 1_000_000 {
		return out, fmt.Errorf("rocksdb: invalid wallet note summary query")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}

	snapshot := s.db.NewSnapshot()
	defer func() { _ = snapshot.Close() }()

	walletBytes, closer, err := snapshot.Get(keyWallet(walletID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return out, nil
		}
		return out, fmt.Errorf("rocksdb: wallet note summary wallet: %w", err)
	}
	var wallet walletRecord
	if err := json.Unmarshal(walletBytes, &wallet); err != nil {
		_ = closer.Close()
		return out, fmt.Errorf("rocksdb: wallet note summary wallet decode: %w", err)
	}
	_ = closer.Close()
	if wallet.DisabledAtUnix != nil {
		return out, nil
	}
	out.WalletFound = true

	tipIter, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: blockPrefix,
		UpperBound: prefixUpperBound(blockPrefix),
	})
	if err != nil {
		return out, fmt.Errorf("rocksdb: wallet note summary tip iter: %w", err)
	}
	if !tipIter.Last() {
		iterErr := tipIter.Error()
		_ = tipIter.Close()
		if iterErr != nil {
			return out, fmt.Errorf("rocksdb: wallet note summary tip: %w", iterErr)
		}
		return out, nil
	}
	tipHeight, err := parseFixed20Int64(bytes.TrimPrefix(tipIter.Key(), blockPrefix))
	if err != nil {
		_ = tipIter.Close()
		return out, fmt.Errorf("rocksdb: wallet note summary tip key: %w", err)
	}
	if tipHeight < 0 {
		_ = tipIter.Close()
		return out, store.ErrInvalidWalletNoteState
	}
	var tipRecord blockRecord
	if err := json.Unmarshal(tipIter.Value(), &tipRecord); err != nil || strings.TrimSpace(tipRecord.Hash) == "" {
		_ = tipIter.Close()
		return out, store.ErrInvalidWalletNoteState
	}
	if err := tipIter.Close(); err != nil {
		return out, fmt.Errorf("rocksdb: wallet note summary tip close: %w", err)
	}
	out.TipFound = true
	out.AsOfScannerHeight = tipHeight
	out.AsOfScannerHash = tipRecord.Hash

	stateBytes, stateCloser, err := snapshot.Get(keyWalletUnspentState(walletID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return out, store.ErrInvalidWalletNoteState
		}
		return out, fmt.Errorf("rocksdb: wallet note summary state: %w", err)
	}
	if len(stateBytes) != 8 {
		_ = stateCloser.Close()
		return out, store.ErrInvalidWalletNoteState
	}
	expectedCountRaw := binary.BigEndian.Uint64(stateBytes)
	_ = stateCloser.Close()
	if expectedCountRaw > uint64(^uint64(0)>>1) {
		return out, store.ErrInvalidWalletNoteState
	}
	expectedCount := int64(expectedCountRaw)

	unspentPrefix := keyWalletUnspentNotePrefix(walletID)
	iter, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: unspentPrefix,
		UpperBound: prefixUpperBound(unspentPrefix),
	})
	if err != nil {
		return out, fmt.Errorf("rocksdb: wallet note summary unspent iter: %w", err)
	}
	defer iter.Close()

	var indexCount int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if indexCount >= expectedCount {
			return out, store.ErrInvalidWalletNoteState
		}
		if indexCount >= int64(maxNotes) {
			return out, store.ErrWalletNoteSummaryLimit
		}
		noteKey := append([]byte{}, iter.Value()...)
		if len(noteKey) == 0 {
			return out, store.ErrInvalidWalletNoteState
		}
		noteBytes, noteCloser, err := snapshot.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return out, store.ErrInvalidWalletNoteState
			}
			return out, fmt.Errorf("rocksdb: wallet note summary note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(noteBytes, &rec); err != nil {
			_ = noteCloser.Close()
			return out, fmt.Errorf("rocksdb: wallet note summary note decode: %w", err)
		}
		_ = noteCloser.Close()
		if rec.WalletID != walletID || rec.Height < 0 || rec.ActionIndex < 0 || rec.SpentHeight != nil ||
			!bytes.Equal(noteKey, keyNote(walletID, rec.TxID, rec.ActionIndex)) ||
			!bytes.Equal(iter.Key(), keyWalletUnspentNote(walletID, rec.TxID, rec.ActionIndex)) {
			return out, store.ErrInvalidWalletNoteState
		}
		_, indexCloser, err := snapshot.Get(keyWalletNoteIndex(uint64(rec.Height), walletID, rec.TxID, rec.ActionIndex))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return out, store.ErrInvalidWalletNoteState
			}
			return out, fmt.Errorf("rocksdb: wallet note summary index: %w", err)
		}
		_ = indexCloser.Close()
		if strings.TrimSpace(rec.NoteNullifier) == "" {
			return out, store.ErrInvalidWalletNoteState
		}
		heightIndexValue, heightIndexCloser, err := snapshot.Get(keyNoteHeightIndex(uint64(rec.Height), walletID, rec.TxID, rec.ActionIndex))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return out, store.ErrInvalidWalletNoteState
			}
			return out, fmt.Errorf("rocksdb: wallet note summary height index: %w", err)
		}
		if string(heightIndexValue) != rec.NoteNullifier {
			_ = heightIndexCloser.Close()
			return out, store.ErrInvalidWalletNoteState
		}
		_ = heightIndexCloser.Close()
		nullifierNoteKey, nullifierCloser, err := snapshot.Get(keyNullifier(rec.NoteNullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return out, store.ErrInvalidWalletNoteState
			}
			return out, fmt.Errorf("rocksdb: wallet note summary nullifier index: %w", err)
		}
		if !bytes.Equal(nullifierNoteKey, noteKey) {
			_ = nullifierCloser.Close()
			return out, store.ErrInvalidWalletNoteState
		}
		_ = nullifierCloser.Close()
		if err := addNoteToSummary(&out, rec, tipHeight, minConfirmations, minNoteZat); err != nil {
			return out, err
		}
		indexCount++
	}
	if err := iter.Error(); err != nil {
		return out, fmt.Errorf("rocksdb: wallet note summary unspent iter: %w", err)
	}
	if indexCount != expectedCount || out.TotalUnspent.NoteCount != expectedCount {
		return out, store.ErrInvalidWalletNoteState
	}
	return out, nil
}

func addNoteToSummary(out *store.WalletNoteSummary, rec noteRecord, tipHeight, minConfirmations, minNoteZat int64) error {
	if rec.ActionIndex < 0 || rec.Height < 0 || rec.Height > tipHeight || rec.ValueZat < 0 || rec.SpentTxID != nil || rec.SpentConfirmedHeight != nil ||
		(rec.Position != nil && *rec.Position > uint64(^uint32(0))) ||
		(rec.PendingSpentExpiryHeight != nil && *rec.PendingSpentExpiryHeight < 0) ||
		(rec.PendingSpentTxID == nil && (rec.PendingSpentAtUnix != nil || rec.PendingSpentExpiryHeight != nil)) ||
		(rec.PendingSpentTxID != nil && rec.PendingSpentAtUnix == nil) {
		return store.ErrInvalidWalletNoteState
	}
	if err := addNoteValue(&out.TotalUnspent, rec.ValueZat); err != nil {
		return err
	}

	switch {
	case rec.PendingSpentTxID != nil:
		if err := addNoteValue(&out.PendingSpend.NoteValueSummary, rec.ValueZat); err != nil {
			return err
		}
		if rec.PendingSpentExpiryHeight != nil {
			expiry := *rec.PendingSpentExpiryHeight
			out.PendingSpend.KnownExpiryCount++
			if out.PendingSpend.NextExpiryHeight == nil || expiry < *out.PendingSpend.NextExpiryHeight {
				out.PendingSpend.NextExpiryHeight = int64Pointer(expiry)
			}
			if out.PendingSpend.LastExpiryHeight == nil || expiry > *out.PendingSpend.LastExpiryHeight {
				out.PendingSpend.LastExpiryHeight = int64Pointer(expiry)
			}
		}
	case noteIsImmature(rec.Height, tipHeight, minConfirmations):
		return addNoteValue(&out.Immature, rec.ValueZat)
	case rec.Position == nil:
		return addNoteValue(&out.WitnessUnavailable, rec.ValueZat)
	case rec.ValueZat == 0 || rec.ValueZat < minNoteZat:
		return addNoteValue(&out.BelowMinNote, rec.ValueZat)
	default:
		if err := addNoteValue(&out.Spendable.NoteValueSummary, rec.ValueZat); err != nil {
			return err
		}
		if out.Spendable.SmallestNoteZat == nil || rec.ValueZat < *out.Spendable.SmallestNoteZat {
			out.Spendable.SmallestNoteZat = int64Pointer(rec.ValueZat)
		}
		if out.Spendable.LargestNoteZat == nil || rec.ValueZat > *out.Spendable.LargestNoteZat {
			out.Spendable.LargestNoteZat = int64Pointer(rec.ValueZat)
		}
	}
	return nil
}

func noteIsImmature(noteHeight, tipHeight, minConfirmations int64) bool {
	if minConfirmations == 0 {
		return false
	}
	if noteHeight > tipHeight {
		return true
	}
	return tipHeight-noteHeight < minConfirmations-1
}

func addNoteValue(bucket *store.NoteValueSummary, valueZat int64) error {
	if valueZat < 0 || bucket.ValueZat > int64(^uint64(0)>>1)-valueZat {
		return store.ErrInvalidWalletNoteState
	}
	bucket.NoteCount++
	bucket.ValueZat += valueZat
	return nil
}

func int64Pointer(value int64) *int64 {
	return &value
}

func (s *Store) AddressBalance(ctx context.Context, walletID, recipientAddress string, minConfirmations, scannerHeight int64) (store.AddressBalance, error) {
	_ = ctx
	var out store.AddressBalance
	walletBytes, closer, err := s.db.Get(keyWallet(walletID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return out, nil
		}
		return out, fmt.Errorf("rocksdb: address balance wallet: %w", err)
	}
	var wallet walletRecord
	if err := json.Unmarshal(walletBytes, &wallet); err != nil {
		_ = closer.Close()
		return out, fmt.Errorf("rocksdb: address balance wallet decode: %w", err)
	}
	_ = closer.Close()
	if wallet.DisabledAtUnix != nil {
		return out, nil
	}
	out.WalletFound = true

	confirmedThrough := scannerHeight - minConfirmations + 1
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyWalletAddressNoteIndexMin(walletID, recipientAddress),
		UpperBound: prefixUpperBound(keyWalletAddressNotePrefix(walletID, recipientAddress)),
	})
	if err != nil {
		return out, fmt.Errorf("rocksdb: address balance iter: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		noteKey := append([]byte{}, iter.Value()...)
		v, valueCloser, err := s.db.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return out, fmt.Errorf("rocksdb: address balance note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = valueCloser.Close()
			return out, fmt.Errorf("rocksdb: address balance decode: %w", err)
		}
		_ = valueCloser.Close()
		if rec.SpentHeight != nil {
			continue
		}
		out.TotalUnspentZat += rec.ValueZat
		if rec.PendingSpentTxID != nil {
			out.PendingOutgoingZat += rec.ValueZat
		} else if rec.Height > confirmedThrough {
			out.PendingIncomingZat += rec.ValueZat
		} else {
			out.AvailableZat += rec.ValueZat
		}
	}
	if err := iter.Error(); err != nil {
		return out, fmt.Errorf("rocksdb: address balance iter: %w", err)
	}
	return out, nil
}

func (s *Store) ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]store.Note, error) {
	notes, _, err := s.ListWalletNotesPage(ctx, walletID, store.NotesQuery{
		OnlyUnspent: onlyUnspent,
		Limit:       limit,
	})
	return notes, err
}

func (s *Store) ListWalletOutgoingOutputsPage(ctx context.Context, walletID string, query store.OutgoingOutputsQuery) ([]store.OutgoingOutput, *store.NotesCursor, error) {
	_ = ctx
	query = sanitizeOutgoingOutputsQuery(query)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyWalletOutgoingMinedIndexMin(walletID),
		UpperBound: prefixUpperBound(keyWalletOutgoingMinedPrefix(walletID)),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("rocksdb: iter outgoing outputs page: %w", err)
	}
	defer iter.Close()

	if query.Cursor == nil {
		iter.First()
	} else {
		cursorKey := keyWalletOutgoingMinedIndex(walletID, uint64(query.Cursor.Height), query.Cursor.TxID, query.Cursor.ActionIndex)
		iter.SeekGE(cursorKey)
		if iter.Valid() && bytes.Equal(iter.Key(), cursorKey) && !query.IncludeCursor {
			iter.Next()
		}
	}

	out := make([]store.OutgoingOutput, 0, query.Limit+1)
	for ; iter.Valid(); iter.Next() {
		height, txid, actionIndex, err := parseWalletOutgoingMinedIndexKey(iter.Key(), walletID)
		if err != nil {
			return nil, nil, err
		}

		ooKey := keyOutgoingOutput(walletID, txid, actionIndex)
		v, closer, err := s.db.Get(ooKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, nil, fmt.Errorf("rocksdb: get outgoing output: %w", err)
		}
		var rec outgoingOutputRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, nil, fmt.Errorf("rocksdb: decode outgoing output: %w", err)
		}
		_ = closer.Close()

		if query.MinValueZat > 0 && rec.ValueZat < query.MinValueZat {
			continue
		}

		var minedHeight *int64
		h := height
		minedHeight = &h
		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
		}
		var confirmedHeight *int64
		if rec.ConfirmedHeight != nil {
			c := *rec.ConfirmedHeight
			confirmedHeight = &c
		}
		var mempoolSeenAt *time.Time
		if rec.MempoolSeenUnix != nil {
			t := time.Unix(*rec.MempoolSeenUnix, 0).UTC()
			mempoolSeenAt = &t
		}
		var txExpiryHeight *int64
		if rec.TxExpiryHeight != nil {
			eh := *rec.TxExpiryHeight
			txExpiryHeight = &eh
		}
		var expiredAt *time.Time
		if rec.ExpiredAtUnix != nil {
			tm := time.Unix(*rec.ExpiredAtUnix, 0).UTC()
			expiredAt = &tm
		}

		out = append(out, store.OutgoingOutput{
			WalletID:         walletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      minedHeight,
			Position:         posPtr,
			ConfirmedHeight:  confirmedHeight,
			MempoolSeenAt:    mempoolSeenAt,
			TxExpiryHeight:   txExpiryHeight,
			ExpiredAt:        expiredAt,
			RecipientAddress: rec.RecipientAddress,
			ValueZat:         rec.ValueZat,
			MemoHex:          rec.MemoHex,
			OvkScope:         rec.OvkScope,
			RecipientScope:   rec.RecipientScope,
			CreatedAt:        time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
		if len(out) >= query.Limit+1 {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, nil, fmt.Errorf("rocksdb: list outgoing outputs page: %w", err)
	}

	if len(out) <= query.Limit {
		return out, nil, nil
	}
	last := out[query.Limit-1]
	if last.MinedHeight == nil {
		return nil, nil, errors.New("rocksdb: outgoing output missing mined_height")
	}
	next := &store.NotesCursor{
		Height:      *last.MinedHeight,
		TxID:        last.TxID,
		ActionIndex: last.ActionIndex,
	}
	return out[:query.Limit], next, nil
}

func (s *Store) UpdatePendingSpends(ctx context.Context, pending map[string]store.PendingSpend, chainHeight int64, seenAt time.Time) error {
	_ = ctx
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}
	nowUnix := seenAt.Unix()

	desired := make(map[string]store.PendingSpend, len(pending))
	for nf, ps := range pending {
		nf = strings.ToLower(strings.TrimSpace(nf))
		txid := strings.ToLower(strings.TrimSpace(ps.TxID))
		if nf == "" || txid == "" {
			continue
		}
		var expPtr *int64
		if ps.ExpiryHeight != nil {
			exp := *ps.ExpiryHeight
			expPtr = &exp
		}
		desired[nf] = store.PendingSpend{
			TxID:         txid,
			ExpiryHeight: expPtr,
		}
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

		// Clear pending markers only once the tx is deterministically expired:
		// chainHeight > pending_spent_expiry_height, or if expiry height is unknown.
		shouldClear := true

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

				if rec.PendingSpentExpiryHeight != nil && chainHeight <= *rec.PendingSpentExpiryHeight {
					shouldClear = false
				}
				if shouldClear {
					rec.PendingSpentTxID = nil
					rec.PendingSpentAtUnix = nil
					rec.PendingSpentExpiryHeight = nil

					b, err := json.Marshal(rec)
					if err != nil {
						_ = iter.Close()
						return fmt.Errorf("rocksdb: encode note: %w", err)
					}
					if err := batch.Set(noteKey, b, pebble.NoSync); err != nil {
						_ = iter.Close()
						return fmt.Errorf("rocksdb: clear pending note: %w", err)
					}
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

		if shouldClear {
			keyCopy := append([]byte{}, iter.Key()...)
			if err := batch.Delete(keyCopy, pebble.NoSync); err != nil {
				_ = iter.Close()
				return fmt.Errorf("rocksdb: delete pending key: %w", err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return fmt.Errorf("rocksdb: iter pending: %w", err)
	}
	_ = iter.Close()

	for nf, ps := range desired {
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

		txidCopy := ps.TxID
		var expiryCopy *int64
		if ps.ExpiryHeight != nil {
			v := *ps.ExpiryHeight
			expiryCopy = &v
		}

		if rec.PendingSpentTxID == nil || *rec.PendingSpentTxID != txidCopy {
			rec.PendingSpentTxID = &txidCopy
			rec.PendingSpentAtUnix = &nowUnix
			rec.PendingSpentExpiryHeight = expiryCopy
		} else if rec.PendingSpentAtUnix == nil {
			rec.PendingSpentAtUnix = &nowUnix
			if rec.PendingSpentExpiryHeight == nil && expiryCopy != nil {
				rec.PendingSpentExpiryHeight = expiryCopy
			}
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
			WalletID:                 rec.WalletID,
			TxID:                     rec.TxID,
			ActionIndex:              rec.ActionIndex,
			Height:                   rec.Height,
			Position:                 posPtr,
			DiversifierIndex:         rec.DiversifierIndex,
			RecipientAddress:         rec.RecipientAddress,
			ValueZat:                 rec.ValueZat,
			MemoHex:                  rec.MemoHex,
			NoteNullifier:            rec.NoteNullifier,
			PendingSpentTxID:         rec.PendingSpentTxID,
			PendingSpentAt:           pendingAt,
			PendingSpentExpiryHeight: rec.PendingSpentExpiryHeight,
			SpentHeight:              rec.SpentHeight,
			SpentTxID:                rec.SpentTxID,
			ConfirmedHeight:          rec.ConfirmedHeight,
			SpentConfirmedHeight:     rec.SpentConfirmedHeight,
			CreatedAt:                time.Unix(rec.CreatedAtUnix, 0).UTC(),
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
		var posPtr *int64
		if rec.Position != nil {
			p := int64(*rec.Position)
			posPtr = &p
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
		var txExpiryHeight *int64
		if rec.TxExpiryHeight != nil {
			h := *rec.TxExpiryHeight
			txExpiryHeight = &h
		}
		var expiredAt *time.Time
		if rec.ExpiredAtUnix != nil {
			t := time.Unix(*rec.ExpiredAtUnix, 0).UTC()
			expiredAt = &t
		}
		out = append(out, store.OutgoingOutput{
			WalletID:         rec.WalletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MinedHeight:      minedHeight,
			Position:         posPtr,
			ConfirmedHeight:  confirmedHeight,
			MempoolSeenAt:    mempoolSeenAt,
			TxExpiryHeight:   txExpiryHeight,
			ExpiredAt:        expiredAt,
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

func (s *Store) ListOrchardActionHeights(ctx context.Context, startHeight, endHeight int64) ([]int64, error) {
	_ = ctx
	if endHeight < startHeight {
		return nil, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyOrchardActionHeightPrefix(uint64(startHeight)),
		UpperBound: prefixUpperBound(keyOrchardActionHeightPrefix(uint64(endHeight))),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: list orchard action heights iter: %w", err)
	}
	defer iter.Close()
	var out []int64
	last := int64(-1)
	for iter.First(); iter.Valid(); iter.Next() {
		parts := bytes.Split(iter.Key(), []byte("/"))
		if len(parts) != 4 {
			return nil, errors.New("rocksdb: orchard action height index malformed")
		}
		h, err := parseFixed20Int64(parts[1])
		if err != nil {
			return nil, errors.New("rocksdb: orchard action height invalid")
		}
		if h > endHeight {
			break
		}
		if h != last {
			out = append(out, h)
			last = h
		}
	}
	return out, iter.Error()
}

func (s *Store) CountOrchardActionHeights(ctx context.Context) (int64, error) {
	_ = ctx
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: orchardActionHeightPrefix, UpperBound: prefixUpperBound(orchardActionHeightPrefix)})
	if err != nil {
		return 0, fmt.Errorf("rocksdb: count orchard action heights iter: %w", err)
	}
	defer iter.Close()
	var count int64
	last := int64(-1)
	for iter.First(); iter.Valid(); iter.Next() {
		parts := bytes.Split(iter.Key(), []byte("/"))
		if len(parts) != 4 {
			return 0, errors.New("rocksdb: orchard action height index malformed")
		}
		h, err := parseFixed20Int64(parts[1])
		if err != nil {
			return 0, err
		}
		if h != last {
			count++
			last = h
		}
	}
	return count, iter.Error()
}

func (s *Store) ListOrchardActionsAtHeight(ctx context.Context, height int64) ([]store.OrchardAction, error) {
	_ = ctx
	prefix := keyOrchardActionHeightPrefix(uint64(height))
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: list orchard actions iter: %w", err)
	}
	defer iter.Close()
	var out []store.OrchardAction
	for iter.First(); iter.Valid(); iter.Next() {
		parts := bytes.Split(iter.Key(), []byte("/"))
		if len(parts) != 4 {
			return nil, errors.New("rocksdb: orchard action height index malformed")
		}
		actionIndex, err := strconv.ParseInt(string(parts[3]), 10, 32)
		if err != nil {
			return nil, errors.New("rocksdb: orchard action index invalid")
		}
		v, closer, err := s.db.Get(keyOrchardAction(string(parts[2]), int32(actionIndex)))
		if err != nil {
			return nil, fmt.Errorf("rocksdb: get orchard action: %w", err)
		}
		var rec orchardActionRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode orchard action: %w", err)
		}
		_ = closer.Close()
		out = append(out, store.OrchardAction{Height: rec.Height, TxID: rec.TxID, ActionIndex: rec.ActionIndex, ActionNullifier: rec.ActionNullifier, CMX: rec.CMX, EphemeralKey: rec.EphemeralKey, EncCiphertext: rec.EncCiphertext})
	}
	return out, iter.Error()
}

func (s *Store) OrchardTreeSizeAtHeight(ctx context.Context, height int64) (int64, error) {
	_ = ctx
	if height < 0 {
		return 0, nil
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: commitmentPrefix,
		UpperBound: prefixUpperBound(commitmentPrefix),
	})
	if err != nil {
		return 0, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		if errors.Is(iter.Error(), pebble.ErrNotFound) || iter.Error() == nil {
			return 0, nil
		}
		return 0, fmt.Errorf("rocksdb: orchard tree size: %w", iter.Error())
	}

	for iter.Valid() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return 0, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		if rec.Height <= height {
			return int64(rec.Position) + 1, nil
		}
		iter.Prev()
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("rocksdb: orchard tree size: %w", err)
	}
	return 0, nil
}

func (s *Store) ListOrchardCommitmentCMXByPositionRange(ctx context.Context, anchorHeight, startPos, endPos int64) ([]string, error) {
	_ = ctx
	if startPos >= endPos {
		return nil, nil
	}
	if startPos < 0 {
		startPos = 0
	}

	lower := keyCommitment(uint64(startPos))
	upper := keyCommitment(uint64(endPos))
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	capacity := 0
	if endPos > startPos {
		capacity = int(endPos - startPos)
	}
	out := make([]string, 0, capacity)
	for iter.First(); iter.Valid(); iter.Next() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		if rec.Height > anchorHeight {
			break
		}
		out = append(out, rec.CMX)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list commitment cmx range: %w", err)
	}
	return out, nil
}

func (s *Store) ListOrchardCommitmentsByPositionsUpToHeight(ctx context.Context, anchorHeight int64, positions []int64) ([]store.OrchardCommitment, error) {
	_ = ctx
	if len(positions) == 0 {
		return nil, nil
	}

	out := make([]store.OrchardCommitment, 0, len(positions))
	for _, pos := range positions {
		if pos < 0 {
			continue
		}
		v, closer, err := s.db.Get(keyCommitment(uint64(pos)))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get commitment: %w", err)
		}
		var rec commitmentRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return nil, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		_ = closer.Close()
		if rec.Height > anchorHeight {
			continue
		}
		out = append(out, store.OrchardCommitment{
			Position:    int64(rec.Position),
			Height:      rec.Height,
			TxID:        rec.TxID,
			ActionIndex: rec.ActionIndex,
			CMX:         rec.CMX,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Position < out[j].Position })
	return out, nil
}

func (s *Store) ListOrchardSubtreeRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardSubtreeRoot, error) {
	_ = ctx
	if limit <= 0 {
		return nil, nil
	}
	if startIndex < 0 {
		startIndex = 0
	}

	lower := keySubtreeRoot(uint64(startIndex))
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: prefixUpperBound(subtreeRootPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	out := make([]store.OrchardSubtreeRoot, 0, limit)
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		var rec subtreeRootRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode subtree root: %w", err)
		}
		out = append(out, store.OrchardSubtreeRoot{
			SubtreeIndex: int64(rec.SubtreeIndex),
			EndPosition:  int64(rec.EndPosition),
			EndHeight:    rec.EndHeight,
			EndBlockHash: rec.EndBlockHash,
			Root:         rec.Root,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list subtree roots: %w", err)
	}
	return out, nil
}

func (s *Store) ListOrchardShardRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardShardRoot, error) {
	_ = ctx
	if limit <= 0 {
		return nil, nil
	}
	if startIndex < 0 {
		startIndex = 0
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: keyShardRoot(uint64(startIndex)),
		UpperBound: prefixUpperBound(shardRootPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: list shard roots iter: %w", err)
	}
	defer iter.Close()
	out := make([]store.OrchardShardRoot, 0, limit)
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		var rec shardRootRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode shard root: %w", err)
		}
		out = append(out, store.OrchardShardRoot{
			Version: rec.Version, ShardIndex: int64(rec.ShardIndex), EndPosition: int64(rec.EndPosition),
			EndHeight: rec.EndHeight, EndBlockHash: rec.EndBlockHash, Root: rec.Root,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list shard roots: %w", err)
	}
	return out, nil
}

func (s *Store) OrchardShardBackfillCursor(ctx context.Context) (version int32, nextIndex int64, err error) {
	_ = ctx
	v, closer, err := s.db.Get(keyMeta("orchard_shard_cache_cursor"))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 1, 0, nil
		}
		return 0, 0, fmt.Errorf("rocksdb: shard cache cursor: %w", err)
	}
	defer closer.Close()
	var rec shardCursorRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return 0, 0, fmt.Errorf("rocksdb: decode shard cache cursor: %w", err)
	}
	return rec.Version, rec.NextIndex, nil
}

func (s *Store) SetOrchardShardBackfillCursor(ctx context.Context, version int32, nextIndex int64) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(shardCursorRecord{Version: version, NextIndex: nextIndex})
	if err != nil {
		return fmt.Errorf("rocksdb: encode shard cache cursor: %w", err)
	}
	if err := s.db.Set(keyMeta("orchard_shard_cache_cursor"), b, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: set shard cache cursor: %w", err)
	}
	return nil
}

func (s *Store) ApplyOrchardShardRoot(ctx context.Context, r store.OrchardShardRoot, nextIndex int64) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := shardRootRecord{Version: r.Version, ShardIndex: uint64(r.ShardIndex), EndPosition: uint64(r.EndPosition), EndHeight: r.EndHeight, EndBlockHash: r.EndBlockHash, Root: r.Root}
	rootBytes, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode shard root: %w", err)
	}
	cursorBytes, err := json.Marshal(shardCursorRecord{Version: r.Version, NextIndex: nextIndex})
	if err != nil {
		return fmt.Errorf("rocksdb: encode shard cache cursor: %w", err)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(keyShardRoot(uint64(r.ShardIndex)), rootBytes, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: apply shard root: %w", err)
	}
	if err := batch.Set(keyMeta("orchard_shard_cache_cursor"), cursorBytes, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: advance shard cursor: %w", err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: shard root commit: %w", err)
	}
	return nil
}

type rocksTx struct {
	db    *pebble.DB
	batch *pebble.Batch
	now   time.Time

	nextPosLoaded bool
	nextPos       uint64
	maxPos        uint64
}

func (t *rocksTx) AssertCanonicalBlock(ctx context.Context, height int64, hash string) error {
	_ = ctx
	if height < 0 {
		return store.ErrCanonicalBlockChanged
	}
	v, closer, err := t.batch.Get(keyBlock(height))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return store.ErrCanonicalBlockChanged
		}
		return fmt.Errorf("rocksdb: read canonical block: %w", err)
	}
	defer closer.Close()
	var rec blockRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return fmt.Errorf("rocksdb: decode canonical block: %w", err)
	}
	if rec.Hash != hash {
		return store.ErrCanonicalBlockChanged
	}
	return nil
}

func (t *rocksTx) AdvanceCompleteWalletBackfillProgress(ctx context.Context, scannedHeight int64) error {
	_ = ctx
	if scannedHeight < 0 {
		return nil
	}
	iter, err := t.batch.NewIter(&pebble.IterOptions{LowerBound: walletBackfillPrefix, UpperBound: prefixUpperBound(walletBackfillPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: advance complete backfill iterator: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var rec walletBackfillRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: advance complete backfill decode: %w", err)
		}
		if rec.State != "complete" || rec.Generation < 1 || rec.NextHeight < rec.BirthdayHeight || rec.NextHeight > scannedHeight {
			continue
		}
		rec.NextHeight = scannedHeight + 1
		if rec.TargetHeight < scannedHeight {
			rec.TargetHeight = scannedHeight
		}
		rec.UpdatedAtUnix = t.now.Unix()
		encoded, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := t.batch.Set(append([]byte{}, iter.Key()...), encoded, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: advance complete backfill: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: advance complete backfill iterator: %w", err)
	}
	return nil
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

func (t *rocksTx) InsertOrchardSubtreeRoot(ctx context.Context, r store.OrchardSubtreeRoot) error {
	_ = ctx
	if r.SubtreeIndex < 0 {
		return errors.New("rocksdb: negative subtree index")
	}
	if r.EndPosition < 0 {
		return errors.New("rocksdb: negative subtree end position")
	}
	if r.EndHeight < 0 {
		return errors.New("rocksdb: negative subtree end height")
	}

	rec := subtreeRootRecord{
		SubtreeIndex: uint64(r.SubtreeIndex),
		EndPosition:  uint64(r.EndPosition),
		EndHeight:    r.EndHeight,
		EndBlockHash: r.EndBlockHash,
		Root:         r.Root,
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("rocksdb: encode subtree root: %w", err)
	}
	if err := t.batch.Set(keySubtreeRoot(uint64(r.SubtreeIndex)), v, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: insert subtree root: %w", err)
	}
	return nil
}

func (t *rocksTx) ListOrchardCommitmentCMXByPositionRange(ctx context.Context, startPos, endPos int64) ([]string, error) {
	_ = ctx
	if startPos >= endPos {
		return nil, nil
	}
	if startPos < 0 {
		startPos = 0
	}

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: keyCommitment(uint64(startPos)),
		UpperBound: keyCommitment(uint64(endPos)),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	capacity := 0
	if endPos > startPos {
		capacity = int(endPos - startPos)
	}
	out := make([]string, 0, capacity)
	for iter.First(); iter.Valid(); iter.Next() {
		var rec commitmentRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode commitment: %w", err)
		}
		out = append(out, rec.CMX)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: list commitment cmx range tx: %w", err)
	}
	return out, nil
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
		locBytes, closer, err := t.batch.Get(keyNullifier(nf))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("rocksdb: get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

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

		if rec.SpentHeight != nil {
			continue
		}
		if rec.WalletID == "" || rec.ActionIndex < 0 || rec.NoteNullifier != nf || !bytes.Equal(noteKey, keyNote(rec.WalletID, rec.TxID, rec.ActionIndex)) {
			return nil, store.ErrInvalidWalletNoteState
		}
		unspentIndexKey := keyWalletUnspentNote(rec.WalletID, rec.TxID, rec.ActionIndex)
		indexedNoteKey, indexCloser, err := t.batch.Get(unspentIndexKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil, store.ErrInvalidWalletNoteState
			}
			return nil, fmt.Errorf("rocksdb: read wallet unspent note index: %w", err)
		}
		if !bytes.Equal(indexedNoteKey, noteKey) {
			_ = indexCloser.Close()
			return nil, store.ErrInvalidWalletNoteState
		}
		_ = indexCloser.Close()
		rec.SpentHeight = &height
		rec.SpentTxID = &txid
		rec.SpentConfirmedHeight = nil
		rec.PendingSpentTxID = nil
		rec.PendingSpentAtUnix = nil
		rec.PendingSpentExpiryHeight = nil

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: encode note: %w", err)
		}
		if err := t.batch.Set(noteKey, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: mark spent: %w", err)
		}
		if err := t.batch.Delete(unspentIndexKey, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: delete wallet unspent note index: %w", err)
		}
		if err := adjustWalletUnspentCount(t.batch, rec.WalletID, -1); err != nil {
			return nil, err
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
			IsInternal:           noteRecordIsInternal(rec),
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

func (t *rocksTx) ExpireOutgoingOutputs(ctx context.Context, chainHeight int64, expiredAt time.Time) ([]store.OutgoingOutput, error) {
	_ = ctx
	if chainHeight < 0 {
		return nil, errors.New("rocksdb: negative chain height")
	}
	if expiredAt.IsZero() {
		expiredAt = time.Now().UTC()
	}
	expiredAt = expiredAt.UTC()
	expiredUnix := expiredAt.Unix()

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: outgoingOutputPrefix,
		UpperBound: prefixUpperBound(outgoingOutputPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("rocksdb: iter outgoing outputs: %w", err)
	}
	defer iter.Close()

	var out []store.OutgoingOutput
	for iter.First(); iter.Valid(); iter.Next() {
		keyCopy := append([]byte{}, iter.Key()...)

		var rec outgoingOutputRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, fmt.Errorf("rocksdb: decode outgoing output: %w", err)
		}

		if rec.MinedHeight != nil {
			continue
		}
		if rec.ExpiredAtUnix != nil {
			continue
		}
		if rec.MempoolSeenUnix == nil {
			continue
		}
		if rec.TxExpiryHeight == nil {
			continue
		}
		if chainHeight <= *rec.TxExpiryHeight {
			continue
		}

		u := expiredUnix
		rec.ExpiredAtUnix = &u
		h := chainHeight
		rec.ExpiredHeight = &h

		b, err := json.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("rocksdb: encode outgoing output: %w", err)
		}
		if err := t.batch.Set(keyCopy, b, pebble.NoSync); err != nil {
			return nil, fmt.Errorf("rocksdb: expire outgoing output: %w", err)
		}

		mempoolSeenAt := time.Unix(*rec.MempoolSeenUnix, 0).UTC()
		txExpiryHeight := *rec.TxExpiryHeight
		expiredAtCopy := expiredAt
		out = append(out, store.OutgoingOutput{
			WalletID:         rec.WalletID,
			TxID:             rec.TxID,
			ActionIndex:      rec.ActionIndex,
			MempoolSeenAt:    &mempoolSeenAt,
			TxExpiryHeight:   &txExpiryHeight,
			ExpiredAt:        &expiredAtCopy,
			RecipientAddress: rec.RecipientAddress,
			ValueZat:         rec.ValueZat,
			MemoHex:          rec.MemoHex,
			OvkScope:         rec.OvkScope,
			RecipientScope:   rec.RecipientScope,
			CreatedAt:        time.Unix(rec.CreatedAtUnix, 0).UTC(),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("rocksdb: expire outgoing outputs iter: %w", err)
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
	if v, closer, err := t.batch.Get(key); err == nil {
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return false, fmt.Errorf("rocksdb: decode existing note: %w", err)
		}
		_ = closer.Close()
		if rec.WalletID != n.WalletID || rec.TxID != n.TxID || rec.ActionIndex != n.ActionIndex || !bytes.Equal(key, keyNote(rec.WalletID, rec.TxID, rec.ActionIndex)) {
			return false, store.ErrInvalidWalletNoteState
		}
		if _, err := walletUnspentCountFromBatch(t.batch, n.WalletID); err != nil {
			return false, err
		}
		unspentKey := keyWalletUnspentNote(n.WalletID, n.TxID, n.ActionIndex)
		indexedNoteKey, indexCloser, indexErr := t.batch.Get(unspentKey)
		if rec.SpentHeight == nil {
			if indexErr != nil {
				if errors.Is(indexErr, pebble.ErrNotFound) {
					return false, store.ErrInvalidWalletNoteState
				}
				return false, fmt.Errorf("rocksdb: read existing wallet unspent note index: %w", indexErr)
			}
			if !bytes.Equal(indexedNoteKey, key) {
				_ = indexCloser.Close()
				return false, store.ErrInvalidWalletNoteState
			}
			_ = indexCloser.Close()
		} else if indexErr == nil {
			_ = indexCloser.Close()
			return false, store.ErrInvalidWalletNoteState
		} else if !errors.Is(indexErr, pebble.ErrNotFound) {
			return false, fmt.Errorf("rocksdb: read existing wallet unspent note index: %w", indexErr)
		}
		wantEligible := !n.IsInternal
		if rec.DepositEligible != nil && *rec.DepositEligible == wantEligible {
			return false, nil
		}
		rec.DepositEligible = boolPtr(wantEligible)
		b, err := json.Marshal(rec)
		if err != nil {
			return false, fmt.Errorf("rocksdb: encode existing note: %w", err)
		}
		if err := t.batch.Set(key, b, pebble.NoSync); err != nil {
			return false, fmt.Errorf("rocksdb: reclassify note: %w", err)
		}
		return true, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return false, fmt.Errorf("rocksdb: get note: %w", err)
	}

	rec := noteRecord{
		WalletID:         n.WalletID,
		TxID:             n.TxID,
		ActionIndex:      n.ActionIndex,
		Height:           n.Height,
		DepositEligible:  boolPtr(!n.IsInternal),
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
	if err := t.batch.Set(keyWalletUnspentNote(n.WalletID, n.TxID, n.ActionIndex), key, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert wallet unspent note index: %w", err)
	}
	if err := adjustWalletUnspentCount(t.batch, n.WalletID, 1); err != nil {
		return false, err
	}
	if err := t.batch.Set(keyWalletAddressNoteIndex(uint64(n.Height), n.WalletID, n.RecipientAddress, n.TxID, n.ActionIndex), key, pebble.NoSync); err != nil {
		return false, fmt.Errorf("rocksdb: insert address note index: %w", err)
	}
	return true, nil
}

func (t *rocksTx) MarkTransactionInternal(ctx context.Context, txid string) (bool, error) {
	_ = ctx
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return false, errors.New("rocksdb: internal txid required")
	}
	repaired := false
	notes, err := t.batch.NewIter(&pebble.IterOptions{LowerBound: notePrefix, UpperBound: prefixUpperBound(notePrefix)})
	if err != nil {
		return false, err
	}
	for notes.First(); notes.Valid(); notes.Next() {
		var rec noteRecord
		if err := json.Unmarshal(notes.Value(), &rec); err != nil {
			_ = notes.Close()
			return false, err
		}
		if !strings.EqualFold(rec.TxID, txid) || noteRecordIsInternal(rec) {
			continue
		}
		rec.DepositEligible = boolPtr(false)
		b, err := json.Marshal(rec)
		if err != nil {
			_ = notes.Close()
			return false, err
		}
		if err := t.batch.Set(append([]byte{}, notes.Key()...), b, pebble.NoSync); err != nil {
			_ = notes.Close()
			return false, err
		}
		repaired = true
	}
	if err := notes.Error(); err != nil {
		_ = notes.Close()
		return false, err
	}
	_ = notes.Close()

	eventIter, err := t.batch.NewIter(&pebble.IterOptions{LowerBound: eventPrefix, UpperBound: prefixUpperBound(eventPrefix)})
	if err != nil {
		return false, err
	}
	for eventIter.First(); eventIter.Valid(); eventIter.Next() {
		var rec eventRecord
		if err := json.Unmarshal(eventIter.Value(), &rec); err != nil {
			_ = eventIter.Close()
			return false, err
		}
		if !isDepositLifecycleKind(rec.Kind) {
			continue
		}
		var payload struct {
			TxID string `json:"txid"`
		}
		if err := json.Unmarshal([]byte(rec.Payload), &payload); err != nil || !strings.EqualFold(payload.TxID, txid) {
			continue
		}
		id, err := eventIDFromKey(eventIter.Key())
		if err != nil {
			_ = eventIter.Close()
			return false, err
		}
		if err := t.batch.Delete(append([]byte{}, eventIter.Key()...), pebble.NoSync); err != nil {
			_ = eventIter.Close()
			return false, err
		}
		if err := t.batch.Delete(keyEventHeightIndex(uint64(rec.Height), rec.WalletID, id), pebble.NoSync); err != nil {
			_ = eventIter.Close()
			return false, err
		}
		repaired = true
	}
	if err := eventIter.Error(); err != nil {
		_ = eventIter.Close()
		return false, err
	}
	_ = eventIter.Close()
	if repaired {
		epoch, err := store.NewEventEpoch()
		if err != nil {
			return false, err
		}
		if err := t.batch.Set(keyMeta("event_epoch"), []byte(epoch), pebble.NoSync); err != nil {
			return false, err
		}
	}
	return repaired, nil
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
			rec.ExpiredAtUnix = nil
			rec.ExpiredHeight = nil
			if err := t.batch.Set(keyOutgoingMinedHeightIndex(uint64(h), o.WalletID, o.TxID, o.ActionIndex), nil, pebble.NoSync); err != nil {
				return false, fmt.Errorf("rocksdb: insert outgoing mined height index: %w", err)
			}
			if err := t.batch.Set(keyWalletOutgoingMinedIndex(o.WalletID, uint64(h), o.TxID, o.ActionIndex), nil, pebble.NoSync); err != nil {
				return false, fmt.Errorf("rocksdb: insert wallet outgoing mined index: %w", err)
			}
			changed = true
		}
		if rec.Position == nil && o.Position != nil {
			p := uint64(*o.Position)
			rec.Position = &p
			changed = true
		}
		if rec.MempoolSeenUnix == nil && o.MempoolSeenAt != nil && !o.MempoolSeenAt.IsZero() {
			u := o.MempoolSeenAt.UTC().Unix()
			rec.MempoolSeenUnix = &u
			changed = true
		}
		if rec.TxExpiryHeight == nil && o.TxExpiryHeight != nil {
			h := *o.TxExpiryHeight
			rec.TxExpiryHeight = &h
			changed = true
		}
		if rec.ExpiredAtUnix == nil && o.ExpiredAt != nil && !o.ExpiredAt.IsZero() {
			u := o.ExpiredAt.UTC().Unix()
			rec.ExpiredAtUnix = &u
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
	if o.Position != nil {
		p := uint64(*o.Position)
		rec.Position = &p
	}
	if o.TxExpiryHeight != nil {
		h := *o.TxExpiryHeight
		rec.TxExpiryHeight = &h
	}
	if o.ExpiredAt != nil && !o.ExpiredAt.IsZero() {
		u := o.ExpiredAt.UTC().Unix()
		rec.ExpiredAtUnix = &u
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
		if err := t.batch.Set(keyWalletOutgoingMinedIndex(o.WalletID, uint64(*o.MinedHeight), o.TxID, o.ActionIndex), nil, pebble.NoSync); err != nil {
			return false, fmt.Errorf("rocksdb: insert wallet outgoing mined index: %w", err)
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
	BirthdayHeight int64  `json:"birthday_height,omitempty"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
	DisabledAtUnix *int64 `json:"disabled_at_unix,omitempty"`
}

type walletBackfillRecord struct {
	BirthdayHeight int64  `json:"birthday_height"`
	NextHeight     int64  `json:"next_height"`
	TargetHeight   int64  `json:"target_height"`
	State          string `json:"state"`
	LastError      string `json:"last_error,omitempty"`
	Generation     int64  `json:"generation"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
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

type subtreeRootRecord struct {
	SubtreeIndex uint64 `json:"subtree_index"`
	EndPosition  uint64 `json:"end_position"`
	EndHeight    int64  `json:"end_height"`
	EndBlockHash string `json:"end_block_hash"`
	Root         string `json:"root"`
}

type shardRootRecord struct {
	Version      int32  `json:"version"`
	ShardIndex   uint64 `json:"shard_index"`
	EndPosition  uint64 `json:"end_position"`
	EndHeight    int64  `json:"end_height"`
	EndBlockHash string `json:"end_block_hash"`
	Root         string `json:"root"`
}

type shardCursorRecord struct {
	Version   int32 `json:"version"`
	NextIndex int64 `json:"next_index"`
}

type noteRecord struct {
	WalletID                 string  `json:"wallet_id"`
	TxID                     string  `json:"txid"`
	ActionIndex              int32   `json:"action_index"`
	Height                   int64   `json:"height"`
	Position                 *uint64 `json:"position,omitempty"`
	DepositEligible          *bool   `json:"deposit_eligible,omitempty"`
	DiversifierIndex         uint32  `json:"diversifier_index,omitempty"`
	RecipientAddress         string  `json:"recipient_address"`
	ValueZat                 int64   `json:"value_zat"`
	MemoHex                  *string `json:"memo_hex,omitempty"`
	NoteNullifier            string  `json:"note_nullifier"`
	PendingSpentTxID         *string `json:"pending_spent_txid,omitempty"`
	PendingSpentAtUnix       *int64  `json:"pending_spent_at_unix,omitempty"`
	PendingSpentExpiryHeight *int64  `json:"pending_spent_expiry_height,omitempty"`
	SpentHeight              *int64  `json:"spent_height,omitempty"`
	SpentTxID                *string `json:"spent_txid,omitempty"`
	ConfirmedHeight          *int64  `json:"confirmed_height,omitempty"`
	SpentConfirmedHeight     *int64  `json:"spent_confirmed_height,omitempty"`
	CreatedAtUnix            int64   `json:"created_at_unix"`
}

type outgoingOutputRecord struct {
	WalletID string `json:"wallet_id"`
	TxID     string `json:"txid"`

	ActionIndex int32 `json:"action_index"`

	MinedHeight     *int64  `json:"mined_height,omitempty"`
	Position        *uint64 `json:"position,omitempty"`
	ConfirmedHeight *int64  `json:"confirmed_height,omitempty"`
	MempoolSeenUnix *int64  `json:"mempool_seen_at_unix,omitempty"`
	TxExpiryHeight  *int64  `json:"tx_expiry_height,omitempty"`
	ExpiredAtUnix   *int64  `json:"expired_at_unix,omitempty"`
	ExpiredHeight   *int64  `json:"expired_height,omitempty"`

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

func boolPtr(v bool) *bool { return &v }

func noteRecordIsInternal(rec noteRecord) bool {
	return rec.DepositEligible == nil || !*rec.DepositEligible
}

func isDepositLifecycleKind(kind string) bool {
	switch kind {
	case events.KindDepositEvent, events.KindDepositConfirmed, events.KindDepositOrphaned, events.KindDepositUnconfirmed:
		return true
	default:
		return false
	}
}

func eventIDFromKey(key []byte) (uint64, error) {
	i := bytes.LastIndexByte(key, '/')
	if i < 0 || i+1 >= len(key) {
		return 0, errors.New("rocksdb: invalid event key")
	}
	id, err := strconv.ParseUint(string(key[i+1:]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("rocksdb: invalid event key: %w", err)
	}
	return id, nil
}

var (
	walletPrefix                   = []byte("w/")
	walletBackfillPrefix           = []byte("wbf/")
	blockPrefix                    = []byte("b/")
	orchardActionPrefix            = []byte("oa/")
	orchardActionHeightPrefix      = []byte("oah/")
	commitmentPrefix               = []byte("oc/")
	subtreeRootPrefix              = []byte("osr/")
	shardRootPrefix                = []byte("oshr/")
	notePrefix                     = []byte("n/")
	noteHeightPrefix               = []byte("nh/")
	walletNotePrefix               = []byte("nwh/")
	walletAddressNotePrefix        = []byte("nwa/")
	walletUnspentNotePrefix        = []byte("nuw/")
	walletUnspentStatePrefix       = []byte("nus/")
	nullifierPrefix                = []byte("nn/")
	pendingNullifierPrefix         = []byte("np/")
	noteSpentHeightPrefix          = []byte("nsh/")
	noteConfirmedHeightPrefix      = []byte("nch/")
	noteSpentConfirmedHeightPrefix = []byte("nsch/")
	outgoingOutputPrefix           = []byte("oo/")
	outgoingMinedHeightPrefix      = []byte("oomh/")
	outgoingWalletMinedPrefix      = []byte("oowh/")
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

func keyWalletBackfill(walletID string) []byte {
	b := make([]byte, 0, len(walletBackfillPrefix)+len(walletID))
	b = append(b, walletBackfillPrefix...)
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

func keyOrchardActionHeightPrefix(height uint64) []byte {
	b := make([]byte, 0, len(orchardActionHeightPrefix)+20+1)
	b = append(b, orchardActionHeightPrefix...)
	b = appendUint64Fixed20(b, height)
	b = append(b, '/')
	return b
}

func keyCommitment(position uint64) []byte {
	b := make([]byte, 0, len(commitmentPrefix)+20)
	b = append(b, commitmentPrefix...)
	b = appendUint64Fixed20(b, position)
	return b
}

func keySubtreeRoot(subtreeIndex uint64) []byte {
	b := make([]byte, 0, len(subtreeRootPrefix)+20)
	b = append(b, subtreeRootPrefix...)
	b = appendUint64Fixed20(b, subtreeIndex)
	return b
}

func keyShardRoot(shardIndex uint64) []byte {
	b := make([]byte, 0, len(shardRootPrefix)+20)
	b = append(b, shardRootPrefix...)
	b = appendUint64Fixed20(b, shardIndex)
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

func keyWalletUnspentNotePrefix(walletID string) []byte {
	b := make([]byte, 0, len(walletUnspentNotePrefix)+len(walletID)+1)
	b = append(b, walletUnspentNotePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	return b
}

func keyWalletUnspentNote(walletID, txid string, actionIndex int32) []byte {
	b := keyWalletUnspentNotePrefix(walletID)
	b = append(b, txid...)
	b = append(b, '/')
	b = strconv.AppendInt(b, int64(actionIndex), 10)
	return b
}

func keyWalletUnspentState(walletID string) []byte {
	b := make([]byte, 0, len(walletUnspentStatePrefix)+len(walletID))
	b = append(b, walletUnspentStatePrefix...)
	b = append(b, walletID...)
	return b
}

func keyWalletAddressNotePrefix(walletID, recipientAddress string) []byte {
	b := make([]byte, 0, len(walletAddressNotePrefix)+len(walletID)+1+len(recipientAddress)+1)
	b = append(b, walletAddressNotePrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = append(b, recipientAddress...)
	b = append(b, '/')
	return b
}

func keyWalletAddressNoteIndexMin(walletID, recipientAddress string) []byte {
	b := keyWalletAddressNotePrefix(walletID, recipientAddress)
	b = appendUint64Fixed20(b, 0)
	return b
}

func keyWalletAddressNoteIndex(height uint64, walletID, recipientAddress, txid string, actionIndex int32) []byte {
	b := keyWalletAddressNotePrefix(walletID, recipientAddress)
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

func keyWalletOutgoingMinedPrefix(walletID string) []byte {
	b := make([]byte, 0, len(outgoingWalletMinedPrefix)+len(walletID)+1)
	b = append(b, outgoingWalletMinedPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	return b
}

func keyWalletOutgoingMinedIndexMin(walletID string) []byte {
	return keyWalletOutgoingMinedPrefix(walletID)
}

func keyWalletOutgoingMinedIndex(walletID string, height uint64, txid string, actionIndex int32) []byte {
	b := make([]byte, 0, len(outgoingWalletMinedPrefix)+len(walletID)+1+20+1+len(txid)+1+11)
	b = append(b, outgoingWalletMinedPrefix...)
	b = append(b, walletID...)
	b = append(b, '/')
	b = appendUint64Fixed20(b, height)
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

func walletUnspentCountFromBatch(batch *pebble.Batch, walletID string) (int64, error) {
	v, closer, err := batch.Get(keyWalletUnspentState(walletID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, store.ErrInvalidWalletNoteState
		}
		return 0, fmt.Errorf("rocksdb: read wallet unspent state: %w", err)
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, store.ErrInvalidWalletNoteState
	}
	count := binary.BigEndian.Uint64(v)
	if count > uint64(^uint64(0)>>1) {
		return 0, store.ErrInvalidWalletNoteState
	}
	return int64(count), nil
}

func adjustWalletUnspentCount(batch *pebble.Batch, walletID string, delta int64) error {
	if delta != -1 && delta != 1 {
		return store.ErrInvalidWalletNoteState
	}
	count, err := walletUnspentCountFromBatch(batch, walletID)
	if err != nil {
		return err
	}
	if delta > 0 && count > int64(^uint64(0)>>1)-delta {
		return store.ErrInvalidWalletNoteState
	}
	if delta < 0 && count < -delta {
		return store.ErrInvalidWalletNoteState
	}
	count += delta
	if err := batch.Set(keyWalletUnspentState(walletID), uint64To8(uint64(count)), pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: update wallet unspent state: %w", err)
	}
	return nil
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
		noteKey := keyNote(walletID, txid, actionIndex)
		var rec noteRecord
		if v, closer, getErr := batch.Get(noteKey); getErr == nil {
			if unmarshalErr := json.Unmarshal(v, &rec); unmarshalErr != nil {
				_ = closer.Close()
				return fmt.Errorf("rocksdb: delete note decode: %w", unmarshalErr)
			}
			_ = closer.Close()
		} else if errors.Is(getErr, pebble.ErrNotFound) {
			return store.ErrInvalidWalletNoteState
		} else if !errors.Is(getErr, pebble.ErrNotFound) {
			return fmt.Errorf("rocksdb: delete note get: %w", getErr)
		}
		if rec.WalletID != walletID || rec.TxID != txid || rec.ActionIndex != actionIndex || rec.Height != height {
			return store.ErrInvalidWalletNoteState
		}
		if nullifier == "" || rec.NoteNullifier != nullifier {
			return store.ErrInvalidWalletNoteState
		}

		if err := batch.Delete(k, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete note height index: %w", err)
		}
		if err := batch.Delete(keyWalletNoteIndex(uint64(height), walletID, txid, actionIndex), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete wallet note index: %w", err)
		}
		if rec.RecipientAddress != "" {
			if err := batch.Delete(keyWalletAddressNoteIndex(uint64(height), walletID, rec.RecipientAddress, txid, actionIndex), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete address note index: %w", err)
			}
		}
		unspentKey := keyWalletUnspentNote(walletID, txid, actionIndex)
		if rec.SpentHeight == nil {
			indexedNoteKey, closer, err := batch.Get(unspentKey)
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					return store.ErrInvalidWalletNoteState
				}
				return fmt.Errorf("rocksdb: delete note read unspent index: %w", err)
			}
			if !bytes.Equal(indexedNoteKey, noteKey) {
				_ = closer.Close()
				return store.ErrInvalidWalletNoteState
			}
			_ = closer.Close()
			if err := batch.Delete(unspentKey, pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete wallet unspent note index: %w", err)
			}
			if err := adjustWalletUnspentCount(batch, walletID, -1); err != nil {
				return err
			}
		} else {
			if _, closer, err := batch.Get(unspentKey); err == nil {
				_ = closer.Close()
				return store.ErrInvalidWalletNoteState
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("rocksdb: delete note read unspent index: %w", err)
			}
			if _, err := walletUnspentCountFromBatch(batch, walletID); err != nil {
				return err
			}
		}
		if err := batch.Delete(noteKey, pebble.NoSync); err != nil {
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
			return store.ErrInvalidWalletNoteState
		}
		spentHeight, err := parseFixed20Int64(parts[1])
		if err != nil || spentHeight <= int64(height) {
			return store.ErrInvalidWalletNoteState
		}
		nullifier := string(parts[2])
		if nullifier == "" {
			return store.ErrInvalidWalletNoteState
		}

		locBytes, closer, err := batch.Get(keyNullifier(nullifier))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				oldLocBytes, oldCloser, oldErr := db.Get(keyNullifier(nullifier))
				if oldErr != nil {
					if errors.Is(oldErr, pebble.ErrNotFound) {
						return store.ErrInvalidWalletNoteState
					}
					return fmt.Errorf("rocksdb: unspend verify deleted nullifier: %w", oldErr)
				}
				oldNoteKey := append([]byte{}, oldLocBytes...)
				_ = oldCloser.Close()
				if _, oldNoteCloser, oldErr := db.Get(oldNoteKey); oldErr != nil {
					if errors.Is(oldErr, pebble.ErrNotFound) {
						return store.ErrInvalidWalletNoteState
					}
					return fmt.Errorf("rocksdb: unspend verify deleted note: %w", oldErr)
				} else {
					_ = oldNoteCloser.Close()
				}
				if _, batchNoteCloser, batchErr := batch.Get(oldNoteKey); batchErr == nil {
					_ = batchNoteCloser.Close()
					return store.ErrInvalidWalletNoteState
				} else if !errors.Is(batchErr, pebble.ErrNotFound) {
					return fmt.Errorf("rocksdb: unspend verify batch note deletion: %w", batchErr)
				}
				_ = batch.Delete(k, pebble.NoSync)
				continue
			}
			return fmt.Errorf("rocksdb: unspend get nullifier: %w", err)
		}
		noteKey := append([]byte{}, locBytes...)
		_ = closer.Close()

		v, closer, err := batch.Get(noteKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return store.ErrInvalidWalletNoteState
			}
			return fmt.Errorf("rocksdb: unspend get note: %w", err)
		}
		var rec noteRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unspend decode note: %w", err)
		}
		_ = closer.Close()
		if rec.SpentHeight == nil || *rec.SpentHeight != spentHeight || rec.WalletID == "" || rec.ActionIndex < 0 || rec.NoteNullifier != nullifier || !bytes.Equal(noteKey, keyNote(rec.WalletID, rec.TxID, rec.ActionIndex)) {
			return store.ErrInvalidWalletNoteState
		}
		unspentKey := keyWalletUnspentNote(rec.WalletID, rec.TxID, rec.ActionIndex)
		if _, closer, err := batch.Get(unspentKey); err == nil {
			_ = closer.Close()
			return store.ErrInvalidWalletNoteState
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("rocksdb: unspend read unspent index: %w", err)
		}

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
		if err := batch.Set(unspentKey, noteKey, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unspend set wallet unspent note index: %w", err)
		}
		if err := adjustWalletUnspentCount(batch, rec.WalletID, 1); err != nil {
			return err
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
		if err := batch.Delete(keyWalletOutgoingMinedIndex(walletID, uint64(*rec.MinedHeight), txid, actionIndex), pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: delete wallet outgoing mined index: %w", err)
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

func unexpireOutgoingOutputsAboveHeight(batch *pebble.Batch, db *pebble.DB, height int64) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: outgoingOutputPrefix, UpperBound: prefixUpperBound(outgoingOutputPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: unexpire outgoing outputs iter: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte{}, iter.Key()...)
		value, closer, err := batch.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("rocksdb: unexpire outgoing output get: %w", err)
		}
		var rec outgoingOutputRecord
		if err := json.Unmarshal(value, &rec); err != nil {
			_ = closer.Close()
			return fmt.Errorf("rocksdb: unexpire outgoing output decode: %w", err)
		}
		_ = closer.Close()
		if rec.ExpiredAtUnix == nil || (rec.ExpiredHeight != nil && *rec.ExpiredHeight <= height) {
			continue
		}
		rec.ExpiredAtUnix = nil
		rec.ExpiredHeight = nil
		encoded, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("rocksdb: unexpire outgoing output encode: %w", err)
		}
		if err := batch.Set(key, encoded, pebble.NoSync); err != nil {
			return fmt.Errorf("rocksdb: unexpire outgoing output set: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: unexpire outgoing outputs iter: %w", err)
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

func rollbackSubtreeRoots(batch *pebble.Batch, db *pebble.DB, height uint64) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: subtreeRootPrefix,
		UpperBound: prefixUpperBound(subtreeRootPrefix),
	})
	if err != nil {
		return fmt.Errorf("rocksdb: iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec subtreeRootRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: decode subtree root: %w", err)
		}
		if rec.EndHeight > int64(height) {
			if err := batch.Delete(iter.Key(), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete subtree root: %w", err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: subtree roots scan: %w", err)
	}
	return nil
}

func rollbackShardRoots(batch *pebble.Batch, db *pebble.DB, height int64) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: shardRootPrefix, UpperBound: prefixUpperBound(shardRootPrefix)})
	if err != nil {
		return fmt.Errorf("rocksdb: shard roots iter: %w", err)
	}
	defer iter.Close()
	version := int32(1)
	nextIndex := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		var rec shardRootRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return fmt.Errorf("rocksdb: decode shard root: %w", err)
		}
		if rec.EndHeight > height {
			if err := batch.Delete(append([]byte{}, iter.Key()...), pebble.NoSync); err != nil {
				return fmt.Errorf("rocksdb: delete shard root: %w", err)
			}
			continue
		}
		version = rec.Version
		if candidate := int64(rec.ShardIndex) + 1; candidate > nextIndex {
			nextIndex = candidate
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rocksdb: shard roots scan: %w", err)
	}
	cursor, err := json.Marshal(shardCursorRecord{Version: version, NextIndex: nextIndex})
	if err != nil {
		return err
	}
	if err := batch.Set(keyMeta("orchard_shard_cache_cursor"), cursor, pebble.NoSync); err != nil {
		return fmt.Errorf("rocksdb: rewind shard cache cursor: %w", err)
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
	_, txid, actionIndex, err := parseWalletNoteIndexKey(key, walletID)
	if err != nil {
		return nil, err
	}
	return keyNote(walletID, txid, actionIndex), nil
}

func parseWalletNoteIndexKey(key []byte, walletID string) (int64, string, int32, error) {
	prefix := keyWalletNotePrefix(walletID)
	if !bytes.HasPrefix(key, prefix) {
		return 0, "", 0, errors.New("rocksdb: wallet note index prefix mismatch")
	}
	rest := bytes.TrimPrefix(key, prefix) // <height>/<txid>/<action_index>
	parts := bytes.Split(rest, []byte("/"))
	if len(parts) != 3 {
		return 0, "", 0, errors.New("rocksdb: wallet note index malformed")
	}
	height, err := parseFixed20Int64(parts[0])
	if err != nil {
		return 0, "", 0, errors.New("rocksdb: wallet note height invalid")
	}
	actionIndex, err := strconv.ParseInt(string(parts[2]), 10, 32)
	if err != nil {
		return 0, "", 0, errors.New("rocksdb: wallet note action_index invalid")
	}
	return height, string(parts[1]), int32(actionIndex), nil
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

func parseWalletOutgoingMinedIndexKey(key []byte, wantWalletID string) (height int64, txid string, actionIndex int32, err error) {
	// k = oowh/<wallet>/<height>/<txid>/<action_index>
	parts := bytes.Split(key, []byte("/"))
	if len(parts) != 5 {
		return 0, "", 0, errors.New("rocksdb: wallet outgoing mined index malformed")
	}
	if string(parts[1]) != wantWalletID {
		return 0, "", 0, errors.New("rocksdb: wallet outgoing mined index wallet mismatch")
	}
	h, err := parseFixed20Int64(parts[2])
	if err != nil {
		return 0, "", 0, errors.New("rocksdb: wallet outgoing mined index height invalid")
	}
	action, err := strconv.ParseInt(string(parts[4]), 10, 32)
	if err != nil {
		return 0, "", 0, errors.New("rocksdb: wallet outgoing mined index action_index invalid")
	}
	return h, string(parts[3]), int32(action), nil
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
