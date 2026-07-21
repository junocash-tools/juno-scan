package rocksdb

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/cockroachdb/pebble"
)

func TestEventEpochStablePerDatabaseAndDifferentForFreshDatabase(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/one.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := st.EventEpoch(ctx)
	if err != nil || len(first) != store.EventEpochHexLength {
		t.Fatalf("epoch=%q err=%v", first, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	second, _ := st.EventEpoch(ctx)
	if second != first {
		t.Fatalf("epoch changed across restart: %q != %q", second, first)
	}
	fresh, err := Open(t.TempDir() + "/fresh.db")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	third, _ := fresh.EventEpoch(ctx)
	if third == first {
		t.Fatalf("fresh databases share epoch %q", first)
	}
}

func TestV3UpgradeQuarantinesLegacyDepositProjectionAndRotatesEpoch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/upgrade.db")
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
	txid := strings.Repeat("a", 64)
	payload, _ := json.Marshal(map[string]any{"txid": txid, "origin": "external"})
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: txid, Height: 1, RecipientAddress: "u", ValueZat: 1, NoteNullifier: strings.Repeat("b", 64)}); err != nil {
			return err
		}
		return tx.InsertEvent(ctx, store.Event{Kind: events.KindDepositEvent, WalletID: "hot", Height: 1, Payload: payload})
	}); err != nil {
		t.Fatal(err)
	}
	oldEpoch := strings.Repeat("c", store.EventEpochHexLength)
	if err := st.db.Set(keyMeta("event_epoch"), []byte(oldEpoch), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyMeta("schema_version"), []byte("3"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	newEpoch, _ := st.EventEpoch(ctx)
	if newEpoch == oldEpoch {
		t.Fatal("upgrade did not rotate event epoch")
	}
	evs, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("legacy deposit events remain: %+v", evs)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		notes, err := tx.ConfirmNotes(ctx, 2, 1)
		if err != nil {
			return err
		}
		if len(notes) != 1 || !notes[0].IsInternal {
			t.Fatalf("legacy note not fail-closed: %+v", notes)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInternalTransactionQuarantinePersistsAcrossRestartAndRollback(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/internal.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	txid := strings.Repeat("d", 64)
	payload, _ := json.Marshal(map[string]any{"txid": txid, "origin": "external"})
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: txid, Height: 1, RecipientAddress: "change", ValueZat: 5, NoteNullifier: strings.Repeat("e", 64)}); err != nil {
			return err
		}
		if err := tx.InsertEvent(ctx, store.Event{Kind: events.KindDepositEvent, WalletID: "hot", Height: 1, Payload: payload}); err != nil {
			return err
		}
		_, err := tx.MarkTransactionInternal(ctx, txid)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	epoch, _ := st.EventEpoch(ctx)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	restartedEpoch, _ := st.EventEpoch(ctx)
	if restartedEpoch != epoch {
		t.Fatalf("epoch changed on restart: %q != %q", restartedEpoch, epoch)
	}
	if err := st.RollbackToHeight(ctx, 0); err != nil {
		t.Fatal(err)
	}
	evs, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range evs {
		if isDepositLifecycleKind(event.Kind) {
			t.Fatalf("internal note emitted lifecycle event after rollback: %+v", event)
		}
	}
}
