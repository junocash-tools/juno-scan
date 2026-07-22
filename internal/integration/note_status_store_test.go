//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func exerciseWalletNoteStatuses(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()
	if err := st.UpsertWallet(ctx, "status-wallet", "status-ufvk"); err != nil {
		t.Fatal(err)
	}

	unspent := store.NoteRef{TxID: summaryBackendID('a'), ActionIndex: 0}
	pending := store.NoteRef{TxID: summaryBackendID('b'), ActionIndex: 1}
	spent := store.NoteRef{TxID: summaryBackendID('c'), ActionIndex: 2}
	unknown := store.NoteRef{TxID: summaryBackendID('d'), ActionIndex: 3}
	sticky := store.NoteRef{TxID: summaryBackendID('e'), ActionIndex: 4}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 20, Hash: "status-h20"}); err != nil {
			return err
		}
		for i, item := range []struct {
			ref       store.NoteRef
			nullifier string
			value     int64
		}{
			{unspent, summaryBackendID('1'), 100},
			{pending, summaryBackendID('2'), 200},
			{spent, summaryBackendID('3'), 300},
			{sticky, summaryBackendID('4'), 400},
		} {
			position := int64(i)
			if _, err := tx.InsertNote(ctx, store.Note{
				WalletID:      "status-wallet",
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
		_, err := tx.MarkNotesSpent(ctx, 20, summaryBackendID('f'), []string{summaryBackendID('3')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(30)
	seenAt := time.Unix(1_700_000_000, 0).UTC()
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		summaryBackendID('2'): {TxID: summaryBackendID('9'), ExpiryHeight: &expiry},
		summaryBackendID('4'): {TxID: summaryBackendID('8')},
	}, 20, seenAt); err != nil {
		t.Fatal(err)
	}

	got, err := st.WalletNoteStatuses(ctx, "status-wallet", []store.NoteRef{unknown, spent, pending, sticky, unspent})
	if err != nil {
		t.Fatal(err)
	}
	if !got.WalletFound || !got.TipFound || got.AsOfScannerHeight != 20 || got.AsOfScannerHash != "status-h20" || len(got.EventEpoch) != store.EventEpochHexLength || len(got.Notes) != 4 {
		t.Fatalf("snapshot=%+v", got)
	}
	if _, ok := got.Notes[unknown]; ok {
		t.Fatal("unknown note unexpectedly returned")
	}
	if note := got.Notes[unspent]; note.ValueZat != 100 || note.PendingSpentTxID != nil || note.SpentTxID != nil {
		t.Fatalf("unspent=%+v", note)
	}
	if note := got.Notes[pending]; note.ValueZat != 200 || note.PendingSpentTxID == nil || *note.PendingSpentTxID != summaryBackendID('9') || note.PendingSpentAt == nil || !note.PendingSpentAt.Equal(seenAt) || note.PendingSpentExpiryHeight == nil || *note.PendingSpentExpiryHeight != expiry {
		t.Fatalf("pending=%+v", note)
	}
	if note := got.Notes[sticky]; note.ValueZat != 400 || note.PendingSpentTxID == nil || *note.PendingSpentTxID != summaryBackendID('8') || note.PendingSpentAt == nil || !note.PendingSpentAt.Equal(seenAt) || note.PendingSpentExpiryHeight != nil {
		t.Fatalf("unknown-expiry pending=%+v", note)
	}
	if note := got.Notes[spent]; note.ValueZat != 300 || note.SpentTxID == nil || *note.SpentTxID != summaryBackendID('f') || note.SpentHeight == nil || *note.SpentHeight != 20 {
		t.Fatalf("spent=%+v", note)
	}

	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 31, Hash: "status-h31"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdatePendingSpends(ctx, nil, 31, seenAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	afterExpiry, err := st.WalletNoteStatuses(ctx, "status-wallet", []store.NoteRef{pending, sticky})
	if err != nil {
		t.Fatal(err)
	}
	if note := afterExpiry.Notes[pending]; note.PendingSpentTxID != nil || note.PendingSpentAt != nil || note.PendingSpentExpiryHeight != nil {
		t.Fatalf("known-expiry pending was not cleared: %+v", note)
	}
	if note := afterExpiry.Notes[sticky]; note.PendingSpentTxID == nil || *note.PendingSpentTxID != summaryBackendID('8') || note.PendingSpentAt == nil || note.PendingSpentExpiryHeight != nil {
		t.Fatalf("unknown-expiry pending did not remain sticky: %+v", note)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		_, err := tx.MarkNotesSpent(ctx, 31, summaryBackendID('7'), []string{summaryBackendID('4')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	afterMined, err := st.WalletNoteStatuses(ctx, "status-wallet", []store.NoteRef{sticky})
	if err != nil {
		t.Fatal(err)
	}
	if note := afterMined.Notes[sticky]; note.PendingSpentTxID != nil || note.SpentTxID == nil || *note.SpentTxID != summaryBackendID('7') {
		t.Fatalf("mined sticky pending=%+v", note)
	}
}
