package rocksdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/cockroachdb/pebble"
)

func TestWalletNoteSummaryBucketsAndLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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

	positions := []int64{1, 2, 3, 4, 5, 6}
	notes := []store.Note{
		{WalletID: "hot", TxID: summaryTxID('a'), Height: 91, Position: &positions[0], ValueZat: 100, NoteNullifier: summaryTxID('1')},
		{WalletID: "hot", TxID: summaryTxID('b'), Height: 1, Position: &positions[1], ValueZat: 500, NoteNullifier: summaryTxID('2')},
		{WalletID: "hot", TxID: summaryTxID('c'), Height: 1, Position: &positions[2], ValueZat: 99, NoteNullifier: summaryTxID('3')},
		{WalletID: "hot", TxID: summaryTxID('d'), Height: 1, Position: &positions[3], ValueZat: 0, NoteNullifier: summaryTxID('4')},
		{WalletID: "hot", TxID: summaryTxID('e'), Height: 1, ValueZat: 700, NoteNullifier: summaryTxID('5')},
		{WalletID: "hot", TxID: summaryTxID('f'), Height: 92, Position: &positions[4], ValueZat: 800, NoteNullifier: summaryTxID('6')},
		{WalletID: "hot", TxID: summaryTxID('7'), Height: 1, Position: &positions[5], ValueZat: 900, NoteNullifier: summaryTxID('7')},
		{WalletID: "hot", TxID: summaryTxID('8'), Height: 100, ValueZat: 1000, NoteNullifier: summaryTxID('8')},
		{WalletID: "hot", TxID: summaryTxID('9'), Height: 1, ValueZat: 2000, NoteNullifier: summaryTxID('9')},
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 100, Hash: "h100"}); err != nil {
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
		_, err := tx.MarkNotesSpent(ctx, 100, summaryTxID('0'), []string{summaryTxID('9')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(130)
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		summaryTxID('7'): {TxID: summaryTxID('a'), ExpiryHeight: &expiry},
		summaryTxID('8'): {TxID: summaryTxID('b')},
	}, 100, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	got, err := st.WalletNoteSummary(ctx, "hot", 10, 100, 8)
	if err != nil {
		t.Fatal(err)
	}
	assertWalletNoteSummary(t, got)

	zeroConfirmations, err := st.WalletNoteSummary(ctx, "hot", 0, 100, 8)
	if err != nil {
		t.Fatal(err)
	}
	if zeroConfirmations.Immature.NoteCount != 0 || zeroConfirmations.Spendable.NoteCount != 3 || zeroConfirmations.Spendable.ValueZat != 1400 {
		t.Fatalf("zero-confirmation summary=%+v", zeroConfirmations)
	}

	if _, err := st.WalletNoteSummary(ctx, "hot", 10, 100, 7); !errors.Is(err, store.ErrWalletNoteSummaryLimit) {
		t.Fatalf("limit error=%v", err)
	}
	missing, err := st.WalletNoteSummary(ctx, "missing", 10, 100, 8)
	if err != nil || missing != (store.WalletNoteSummary{}) {
		t.Fatalf("missing summary=%+v err=%v", missing, err)
	}
	walletBytes, closer, err := st.db.Get(keyWallet("hot"))
	if err != nil {
		t.Fatal(err)
	}
	var wallet walletRecord
	if err := json.Unmarshal(walletBytes, &wallet); err != nil {
		_ = closer.Close()
		t.Fatal(err)
	}
	_ = closer.Close()
	disabledAt := time.Now().UTC().Unix()
	wallet.DisabledAtUnix = &disabledAt
	encodedWallet, err := json.Marshal(wallet)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyWallet("hot"), encodedWallet, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	disabled, err := st.WalletNoteSummary(ctx, "hot", 10, 100, 8)
	if err != nil || disabled != (store.WalletNoteSummary{}) {
		t.Fatalf("disabled summary=%+v err=%v", disabled, err)
	}
}

func TestWalletNoteSummaryWithoutTip(t *testing.T) {
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
	got, err := st.WalletNoteSummary(ctx, "hot", 10, 0, 100)
	if err != nil || !got.WalletFound || got.TipFound {
		t.Fatalf("summary=%+v err=%v", got, err)
	}
}

func TestWalletNoteSummaryRejectsCorruptIndexedNotes(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*noteRecord)
		removeRecord bool
		removeIndex  bool
	}{
		{name: "negative action index", mutate: func(rec *noteRecord) { rec.ActionIndex = -1 }},
		{name: "index height mismatch", mutate: func(rec *noteRecord) { rec.Height++ }},
		{name: "index txid mismatch", mutate: func(rec *noteRecord) { rec.TxID = summaryTxID('b') }},
		{name: "index action mismatch", mutate: func(rec *noteRecord) { rec.ActionIndex = 1 }},
		{name: "position outside Orchard tree", mutate: func(rec *noteRecord) { position := uint64(^uint32(0)) + 1; rec.Position = &position }},
		{name: "wallet mismatch", mutate: func(rec *noteRecord) { rec.WalletID = "other" }},
		{name: "confirmed spend without mined spend", mutate: func(rec *noteRecord) { height := int64(10); rec.SpentConfirmedHeight = &height }},
		{name: "missing indexed record", removeRecord: true},
		{name: "missing wallet note index", removeIndex: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			position := int64(1)
			if err := st.WithTx(ctx, func(tx store.Tx) error {
				if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
					return err
				}
				_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), ActionIndex: 0, Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')})
				return err
			}); err != nil {
				t.Fatal(err)
			}

			noteKey := keyNote("hot", summaryTxID('a'), 0)
			if tt.removeRecord {
				if err := st.db.Delete(noteKey, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			} else if tt.removeIndex {
				if err := st.db.Delete(keyWalletNoteIndex(1, "hot", summaryTxID('a'), 0), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			} else {
				raw, closer, err := st.db.Get(noteKey)
				if err != nil {
					t.Fatal(err)
				}
				var rec noteRecord
				if err := json.Unmarshal(raw, &rec); err != nil {
					_ = closer.Close()
					t.Fatal(err)
				}
				_ = closer.Close()
				tt.mutate(&rec)
				encoded, err := json.Marshal(rec)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.db.Set(noteKey, encoded, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 100); !errors.Is(err, store.ErrInvalidWalletNoteState) {
				t.Fatalf("error=%v want ErrInvalidWalletNoteState", err)
			}
		})
	}
}

func TestWalletNoteSummaryRejectsCorruptBoundedIndexState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Store, []byte) error
	}{
		{name: "missing unspent membership", mutate: func(st *Store, _ []byte) error {
			return st.db.Delete(keyWalletUnspentNote("hot", summaryTxID('a'), 0), pebble.NoSync)
		}},
		{name: "malformed low-sorting membership", mutate: func(st *Store, noteKey []byte) error {
			return st.db.Set(keyWalletUnspentNotePrefix("hot"), noteKey, pebble.NoSync)
		}},
		{name: "missing unspent state", mutate: func(st *Store, _ []byte) error {
			return st.db.Delete(keyWalletUnspentState("hot"), pebble.NoSync)
		}},
		{name: "malformed unspent state", mutate: func(st *Store, _ []byte) error {
			return st.db.Set(keyWalletUnspentState("hot"), []byte{1}, pebble.NoSync)
		}},
		{name: "count above actual", mutate: func(st *Store, _ []byte) error {
			return st.db.Set(keyWalletUnspentState("hot"), uint64To8(2), pebble.NoSync)
		}},
		{name: "count above cap but inventory below cap", mutate: func(st *Store, _ []byte) error {
			return st.db.Set(keyWalletUnspentState("hot"), uint64To8(101), pebble.NoSync)
		}},
		{name: "missing height index", mutate: func(st *Store, _ []byte) error {
			return st.db.Delete(keyNoteHeightIndex(1, "hot", summaryTxID('a'), 0), pebble.NoSync)
		}},
		{name: "wrong height index value", mutate: func(st *Store, _ []byte) error {
			return st.db.Set(keyNoteHeightIndex(1, "hot", summaryTxID('a'), 0), []byte("wrong"), pebble.NoSync)
		}},
		{name: "missing nullifier index", mutate: func(st *Store, _ []byte) error {
			return st.db.Delete(keyNullifier(summaryTxID('1')), pebble.NoSync)
		}},
		{name: "wrong nullifier index value", mutate: func(st *Store, _ []byte) error {
			return st.db.Set(keyNullifier(summaryTxID('1')), []byte("wrong"), pebble.NoSync)
		}},
		{name: "spent record remains in unspent index", mutate: func(st *Store, noteKey []byte) error {
			raw, closer, err := st.db.Get(noteKey)
			if err != nil {
				return err
			}
			var rec noteRecord
			if err := json.Unmarshal(raw, &rec); err != nil {
				_ = closer.Close()
				return err
			}
			_ = closer.Close()
			height := int64(10)
			rec.SpentHeight = &height
			encoded, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			return st.db.Set(noteKey, encoded, pebble.NoSync)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			position := int64(1)
			if err := st.WithTx(ctx, func(tx store.Tx) error {
				if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
					return err
				}
				_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			noteKey := keyNote("hot", summaryTxID('a'), 0)
			if err := tt.mutate(st, noteKey); err != nil {
				t.Fatal(err)
			}
			if _, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 100); !errors.Is(err, store.ErrInvalidWalletNoteState) {
				t.Fatalf("error=%v want ErrInvalidWalletNoteState", err)
			}
		})
	}
}

func TestWalletNoteSummaryHotPathDoesNotScanSpentHistory(t *testing.T) {
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

	const spentCount = 200
	spentNullifiers := make([]string, 0, spentCount)
	position := int64(1)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
			return err
		}
		for i := 1; i <= spentCount+1; i++ {
			txid := fmt.Sprintf("%064x", i)
			nullifier := fmt.Sprintf("%064x", 10_000+i)
			if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: txid, Height: 1, Position: &position, ValueZat: 1, NoteNullifier: nullifier}); err != nil {
				return err
			}
			if i <= spentCount {
				spentNullifiers = append(spentNullifiers, nullifier)
			}
		}
		spendRequests := append(append([]string{}, spentNullifiers...), spentNullifiers[0])
		_, err := tx.MarkNotesSpent(ctx, 2, fmt.Sprintf("%064x", 20_000), spendRequests)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Corrupt a spent primary record. A bounded unspent summary must not touch
	// lifetime spent history on its hot path.
	if err := st.db.Set(keyNote("hot", fmt.Sprintf("%064x", 1), 0), []byte("{"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUnspent.NoteCount != 1 || got.TotalUnspent.ValueZat != 1 || got.Spendable.NoteCount != 1 {
		t.Fatalf("bounded summary=%+v", got)
	}
}

func TestWalletUnspentIndexTracksRollbackLifecycle(t *testing.T) {
	t.Run("spend rollback re-adds note", func(t *testing.T) {
		ctx := context.Background()
		st := openSummaryStore(t, ctx)
		defer st.Close()
		position := int64(1)
		if err := st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
				return err
			}
			if err := tx.InsertBlock(ctx, store.Block{Height: 2, Hash: "h2"}); err != nil {
				return err
			}
			if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')}); err != nil {
				return err
			}
			_, err := tx.MarkNotesSpent(ctx, 2, summaryTxID('b'), []string{summaryTxID('1')})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.RollbackToHeight(ctx, 1); err != nil {
			t.Fatal(err)
		}
		got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
		if err != nil || got.TotalUnspent.NoteCount != 1 || got.TotalUnspent.ValueZat != 100 {
			t.Fatalf("rollback summary=%+v err=%v", got, err)
		}
	})

	t.Run("created-above note deletion decrements state", func(t *testing.T) {
		ctx := context.Background()
		st := openSummaryStore(t, ctx)
		defer st.Close()
		position := int64(1)
		if err := st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
				return err
			}
			if err := tx.InsertBlock(ctx, store.Block{Height: 2, Hash: "h2"}); err != nil {
				return err
			}
			_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 2, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.RollbackToHeight(ctx, 1); err != nil {
			t.Fatal(err)
		}
		got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
		if err != nil || got.TotalUnspent.NoteCount != 0 {
			t.Fatalf("rollback summary=%+v err=%v", got, err)
		}
	})

	t.Run("created-and-spent-above note is deleted once", func(t *testing.T) {
		ctx := context.Background()
		st := openSummaryStore(t, ctx)
		defer st.Close()
		position := int64(1)
		if err := st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
				return err
			}
			if err := tx.InsertBlock(ctx, store.Block{Height: 2, Hash: "h2"}); err != nil {
				return err
			}
			if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 2, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')}); err != nil {
				return err
			}
			_, err := tx.MarkNotesSpent(ctx, 2, summaryTxID('b'), []string{summaryTxID('1')})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.RollbackToHeight(ctx, 1); err != nil {
			t.Fatal(err)
		}
		got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
		if err != nil || got.TotalUnspent.NoteCount != 0 {
			t.Fatalf("rollback summary=%+v err=%v", got, err)
		}
	})

	t.Run("destructive reset preserves zero state", func(t *testing.T) {
		ctx := context.Background()
		st := openSummaryStore(t, ctx)
		defer st.Close()
		position := int64(1)
		if err := st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
				return err
			}
			_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.RollbackToHeight(ctx, -1); err != nil {
			t.Fatal(err)
		}
		if err := st.WithTx(ctx, func(tx store.Tx) error { return tx.InsertBlock(ctx, store.Block{Height: 0, Hash: "h0"}) }); err != nil {
			t.Fatal(err)
		}
		got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
		if err != nil || !got.WalletFound || got.TotalUnspent.NoteCount != 0 {
			t.Fatalf("reset summary=%+v err=%v", got, err)
		}
	})
}

func TestV4MigrationBuildsWalletUnspentIndexAtomically(t *testing.T) {
	ctx := context.Background()
	st := openSummaryStore(t, ctx)
	defer st.Close()
	position := int64(1)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('b'), Height: 1, Position: &position, ValueZat: 200, NoteNullifier: summaryTxID('2')}); err != nil {
			return err
		}
		_, err := tx.MarkNotesSpent(ctx, 1, summaryTxID('c'), []string{summaryTxID('2')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.db.DeleteRange(walletUnspentNotePrefix, prefixUpperBound(walletUnspentNotePrefix), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.db.DeleteRange(walletUnspentStatePrefix, prefixUpperBound(walletUnspentStatePrefix), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyMeta("schema_version"), []byte("4"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := st.WalletNoteSummary(ctx, "hot", 1, 0, 1)
	if err != nil || got.TotalUnspent.NoteCount != 1 || got.TotalUnspent.ValueZat != 100 {
		t.Fatalf("migrated summary=%+v err=%v", got, err)
	}
	version, closer, err := st.db.Get(keyMeta("schema_version"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if string(version) != "5" {
		t.Fatalf("schema version=%q", version)
	}
}

func TestV4MigrationRejectsCorruptNoteIndexWithoutAdvancing(t *testing.T) {
	ctx := context.Background()
	st := openSummaryStore(t, ctx)
	defer st.Close()
	position := int64(1)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: summaryTxID('a'), Height: 1, Position: &position, ValueZat: 100, NoteNullifier: summaryTxID('1')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Delete(keyNoteHeightIndex(1, "hot", summaryTxID('a'), 0), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyMeta("schema_version"), []byte("4"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); !errors.Is(err, store.ErrInvalidWalletNoteState) {
		t.Fatalf("migration error=%v want ErrInvalidWalletNoteState", err)
	}
	version, closer, err := st.db.Get(keyMeta("schema_version"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if string(version) != "4" {
		t.Fatalf("failed migration advanced schema to %q", version)
	}
}

func openSummaryStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	return st
}

func assertWalletNoteSummary(t *testing.T, got store.WalletNoteSummary) {
	t.Helper()
	if !got.WalletFound || !got.TipFound || got.AsOfScannerHeight != 100 || got.AsOfScannerHash != "h100" {
		t.Fatalf("snapshot identity=%+v", got)
	}
	if got.TotalUnspent.NoteCount != 8 || got.TotalUnspent.ValueZat != 4099 {
		t.Fatalf("total=%+v", got.TotalUnspent)
	}
	if got.Spendable.NoteCount != 2 || got.Spendable.ValueZat != 600 || got.Spendable.SmallestNoteZat == nil || *got.Spendable.SmallestNoteZat != 100 || got.Spendable.LargestNoteZat == nil || *got.Spendable.LargestNoteZat != 500 {
		t.Fatalf("spendable=%+v", got.Spendable)
	}
	if got.Immature.NoteCount != 1 || got.Immature.ValueZat != 800 {
		t.Fatalf("immature=%+v", got.Immature)
	}
	if got.PendingSpend.NoteCount != 2 || got.PendingSpend.ValueZat != 1900 || got.PendingSpend.KnownExpiryCount != 1 || got.PendingSpend.NextExpiryHeight == nil || *got.PendingSpend.NextExpiryHeight != 130 || got.PendingSpend.LastExpiryHeight == nil || *got.PendingSpend.LastExpiryHeight != 130 {
		t.Fatalf("pending=%+v", got.PendingSpend)
	}
	if got.BelowMinNote.NoteCount != 2 || got.BelowMinNote.ValueZat != 99 {
		t.Fatalf("below floor=%+v", got.BelowMinNote)
	}
	if got.WitnessUnavailable.NoteCount != 1 || got.WitnessUnavailable.ValueZat != 700 {
		t.Fatalf("witness unavailable=%+v", got.WitnessUnavailable)
	}
	partitionCount := got.Spendable.NoteCount + got.Immature.NoteCount + got.PendingSpend.NoteCount + got.BelowMinNote.NoteCount + got.WitnessUnavailable.NoteCount
	partitionValue := got.Spendable.ValueZat + got.Immature.ValueZat + got.PendingSpend.ValueZat + got.BelowMinNote.ValueZat + got.WitnessUnavailable.ValueZat
	if partitionCount != got.TotalUnspent.NoteCount || partitionValue != got.TotalUnspent.ValueZat {
		t.Fatalf("buckets do not partition total: %+v", got)
	}
}

func summaryTxID(fill byte) string {
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = fill
	}
	return string(buf)
}
