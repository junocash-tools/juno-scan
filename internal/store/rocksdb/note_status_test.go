package rocksdb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func TestWalletNoteStatusesPointLookupSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}

	unspent := store.NoteRef{TxID: noteStatusTxID('a'), ActionIndex: 0}
	pending := store.NoteRef{TxID: noteStatusTxID('b'), ActionIndex: 1}
	spent := store.NoteRef{TxID: noteStatusTxID('c'), ActionIndex: 2}
	unknown := store.NoteRef{TxID: noteStatusTxID('d'), ActionIndex: 3}
	overflow := store.NoteRef{TxID: noteStatusTxID('e'), ActionIndex: 1 << 31}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
			return err
		}
		for i, item := range []struct {
			ref       store.NoteRef
			nullifier string
			value     int64
		}{
			{unspent, noteStatusTxID('1'), 100},
			{pending, noteStatusTxID('2'), 200},
			{spent, noteStatusTxID('3'), 300},
		} {
			position := int64(i)
			if _, err := tx.InsertNote(ctx, store.Note{
				WalletID:      "hot",
				TxID:          item.ref.TxID,
				ActionIndex:   int32(item.ref.ActionIndex),
				Height:        int64(i + 1),
				Position:      &position,
				ValueZat:      item.value,
				NoteNullifier: item.nullifier,
			}); err != nil {
				return err
			}
		}
		_, err := tx.MarkNotesSpent(ctx, 10, noteStatusTxID('f'), []string{noteStatusTxID('3')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(25)
	seenAt := time.Unix(1_700_000_000, 0).UTC()
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		noteStatusTxID('2'): {TxID: noteStatusTxID('9'), ExpiryHeight: &expiry},
	}, 10, seenAt); err != nil {
		t.Fatal(err)
	}

	refs := []store.NoteRef{unknown, spent, unspent, pending, overflow}
	got, err := st.WalletNoteStatuses(ctx, "hot", refs)
	if err != nil {
		t.Fatal(err)
	}
	if !got.WalletFound || !got.TipFound || got.AsOfScannerHeight != 10 || got.AsOfScannerHash != "h10" || len(got.EventEpoch) != store.EventEpochHexLength {
		t.Fatalf("snapshot identity=%+v", got)
	}
	if len(got.Notes) != 3 {
		t.Fatalf("known notes=%d want=3", len(got.Notes))
	}
	if _, ok := got.Notes[unknown]; ok {
		t.Fatal("unknown note unexpectedly returned")
	}
	if _, ok := got.Notes[overflow]; ok {
		t.Fatal("uint32 action index outside persisted int32 domain unexpectedly returned")
	}
	if note := got.Notes[unspent]; note.ValueZat != 100 || note.PendingSpentTxID != nil || note.SpentTxID != nil {
		t.Fatalf("unspent=%+v", note)
	}
	if note := got.Notes[pending]; note.ValueZat != 200 || note.PendingSpentTxID == nil || *note.PendingSpentTxID != noteStatusTxID('9') || note.PendingSpentAt == nil || !note.PendingSpentAt.Equal(seenAt) || note.PendingSpentExpiryHeight == nil || *note.PendingSpentExpiryHeight != expiry {
		t.Fatalf("pending=%+v", note)
	}
	if note := got.Notes[spent]; note.ValueZat != 300 || note.SpentTxID == nil || *note.SpentTxID != noteStatusTxID('f') || note.SpentHeight == nil || *note.SpentHeight != 10 {
		t.Fatalf("spent=%+v", note)
	}

	missing, err := st.WalletNoteStatuses(ctx, "missing", refs)
	if err != nil || missing.WalletFound || !missing.TipFound || missing.AsOfScannerHeight != 10 || missing.EventEpoch != got.EventEpoch {
		t.Fatalf("missing snapshot=%+v err=%v", missing, err)
	}
}

func TestWalletNoteStatusesRejectsInvalidQuery(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	valid := store.NoteRef{TxID: noteStatusTxID('a'), ActionIndex: 0}
	for _, refs := range [][]store.NoteRef{
		nil,
		{valid, valid},
		{{TxID: "ABC", ActionIndex: 0}},
	} {
		if _, err := st.WalletNoteStatuses(ctx, "hot", refs); !errors.Is(err, store.ErrInvalidNoteStatusQuery) {
			t.Fatalf("refs=%+v err=%v", refs, err)
		}
	}
}

func TestUpdatePendingSpendsKeepsUnknownExpirySticky(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	ref := store.NoteRef{TxID: noteStatusTxID('a'), ActionIndex: 0}
	nullifier := noteStatusTxID('1')
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
			return err
		}
		_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: ref.TxID, Height: 1, ValueZat: 100, NoteNullifier: nullifier})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	pendingTxID := noteStatusTxID('9')
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		nullifier: {TxID: pendingTxID},
	}, 10, time.Unix(1, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdatePendingSpends(ctx, nil, 1000, time.Unix(2, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.WalletNoteStatuses(ctx, "hot", []store.NoteRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	if note := snapshot.Notes[ref]; note.PendingSpentTxID == nil || *note.PendingSpentTxID != pendingTxID || note.PendingSpentAt == nil || note.PendingSpentExpiryHeight != nil {
		t.Fatalf("unknown-expiry pending=%+v", note)
	}
}

func noteStatusTxID(fill byte) string {
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = fill
	}
	return string(buf)
}
