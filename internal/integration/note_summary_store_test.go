//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func exerciseWalletNoteSummary(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()
	if err := st.UpsertWallet(ctx, "summary-wallet", "summary-ufvk"); err != nil {
		t.Fatal(err)
	}
	positions := []int64{1, 2, 3, 4, 5, 6}
	notes := []store.Note{
		{WalletID: "summary-wallet", TxID: summaryBackendID('a'), Height: 91, Position: &positions[0], ValueZat: 100, NoteNullifier: summaryBackendID('1')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('b'), Height: 1, Position: &positions[1], ValueZat: 500, NoteNullifier: summaryBackendID('2')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('c'), Height: 1, Position: &positions[2], ValueZat: 99, NoteNullifier: summaryBackendID('3')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('d'), Height: 1, Position: &positions[3], ValueZat: 0, NoteNullifier: summaryBackendID('4')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('e'), Height: 1, ValueZat: 700, NoteNullifier: summaryBackendID('5')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('f'), Height: 92, Position: &positions[4], ValueZat: 800, NoteNullifier: summaryBackendID('6')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('7'), Height: 1, Position: &positions[5], ValueZat: 900, NoteNullifier: summaryBackendID('7')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('8'), Height: 100, ValueZat: 1000, NoteNullifier: summaryBackendID('8')},
		{WalletID: "summary-wallet", TxID: summaryBackendID('9'), Height: 1, ValueZat: 2000, NoteNullifier: summaryBackendID('9')},
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 100, Hash: "summary-h100"}); err != nil {
			return err
		}
		for _, note := range notes {
			if _, err := tx.InsertNote(ctx, note); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		_, err := tx.MarkNotesSpent(ctx, 100, summaryBackendID('0'), []string{summaryBackendID('9')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(130)
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		summaryBackendID('7'): {TxID: summaryBackendID('a'), ExpiryHeight: &expiry},
		summaryBackendID('8'): {TxID: summaryBackendID('b')},
	}, 100, time.Unix(1, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	got, err := st.WalletNoteSummary(ctx, "summary-wallet", 10, 100, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !got.WalletFound || !got.TipFound || got.AsOfScannerHeight != 100 || got.AsOfScannerHash != "summary-h100" || got.TotalUnspent.NoteCount != 8 || got.TotalUnspent.ValueZat != 4099 {
		t.Fatalf("summary identity/total=%+v", got)
	}
	if got.Spendable.NoteCount != 2 || got.Spendable.ValueZat != 600 || got.Spendable.SmallestNoteZat == nil || *got.Spendable.SmallestNoteZat != 100 || got.Spendable.LargestNoteZat == nil || *got.Spendable.LargestNoteZat != 500 {
		t.Fatalf("spendable=%+v", got.Spendable)
	}
	if got.Immature.NoteCount != 1 || got.Immature.ValueZat != 800 || got.BelowMinNote.NoteCount != 2 || got.BelowMinNote.ValueZat != 99 || got.WitnessUnavailable.NoteCount != 1 || got.WitnessUnavailable.ValueZat != 700 {
		t.Fatalf("non-pending buckets=%+v", got)
	}
	if got.PendingSpend.NoteCount != 2 || got.PendingSpend.ValueZat != 1900 || got.PendingSpend.KnownExpiryCount != 1 || got.PendingSpend.NextExpiryHeight == nil || *got.PendingSpend.NextExpiryHeight != 130 || got.PendingSpend.LastExpiryHeight == nil || *got.PendingSpend.LastExpiryHeight != 130 {
		t.Fatalf("pending=%+v", got.PendingSpend)
	}
	partitionCount := got.Spendable.NoteCount + got.Immature.NoteCount + got.PendingSpend.NoteCount + got.BelowMinNote.NoteCount + got.WitnessUnavailable.NoteCount
	partitionValue := got.Spendable.ValueZat + got.Immature.ValueZat + got.PendingSpend.ValueZat + got.BelowMinNote.ValueZat + got.WitnessUnavailable.ValueZat
	if partitionCount != got.TotalUnspent.NoteCount || partitionValue != got.TotalUnspent.ValueZat {
		t.Fatalf("buckets do not partition total: %+v", got)
	}
	if _, err := st.WalletNoteSummary(ctx, "summary-wallet", 10, 100, 7); !errors.Is(err, store.ErrWalletNoteSummaryLimit) {
		t.Fatalf("limit error=%v", err)
	}
	missing, err := st.WalletNoteSummary(ctx, "missing-summary-wallet", 10, 100, 8)
	if err != nil || missing != (store.WalletNoteSummary{}) {
		t.Fatalf("missing=%+v err=%v", missing, err)
	}
}

func summaryBackendID(fill byte) string {
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = fill
	}
	return string(buf)
}
