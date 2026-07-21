package rocksdb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/cockroachdb/pebble"
)

func TestEventEpochRotatesOncePerStartupAndIsStableWithinProcess(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/one.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := st.RotateEventEpoch(ctx)
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
	persisted, _ := st.EventEpoch(ctx)
	if persisted != first {
		t.Fatalf("persisted epoch changed before startup rotation: %q != %q", persisted, first)
	}
	second, err := st.RotateEventEpoch(ctx)
	if err != nil || second == first {
		t.Fatalf("startup did not rotate epoch: first=%q second=%q err=%v", first, second, err)
	}
	if stable, _ := st.EventEpoch(ctx); stable != second {
		t.Fatalf("startup epoch unstable within process: %q != %q", stable, second)
	}
	fresh, err := Open(t.TempDir() + "/fresh.db")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	third, _ := fresh.RotateEventEpoch(ctx)
	if third == first || third == second {
		t.Fatalf("fresh databases share epoch %q", first)
	}
}

func TestV3UpgradeRejectsDuplicateUFVKsWithoutAdvancingSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/duplicates.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.db.Set(keyMeta("schema_version"), []byte("3"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyMeta("event_epoch"), []byte(strings.Repeat("a", store.EventEpochHexLength)), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	for _, walletID := range []string{"wallet-a", "wallet-b"} {
		encoded, _ := json.Marshal(walletRecord{UFVK: "duplicate-ufvk", CreatedAtUnix: 1})
		if err := st.db.Set(keyWallet(walletID), encoded, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Migrate(ctx); !errors.Is(err, store.ErrUFVKAlreadyRegistered) {
		t.Fatalf("Migrate duplicate UFVK err=%v", err)
	}
	v, closer, err := st.db.Get(keyMeta("schema_version"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if string(v) != "3" {
		t.Fatalf("failed migration advanced schema to %q", string(v))
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
	restartedEpoch, err := st.RotateEventEpoch(ctx)
	if err != nil || restartedEpoch == epoch {
		t.Fatalf("epoch did not change on startup restart: %q == %q err=%v", restartedEpoch, epoch, err)
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
