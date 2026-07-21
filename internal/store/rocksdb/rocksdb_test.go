package rocksdb

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/cockroachdb/pebble"
)

func TestStore_RollbackUnspendsAndDeletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	st, err := Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if err := st.UpsertWallet(ctx, "hot", "jview1test"); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	payload, _ := json.Marshal(map[string]any{"k": "v"})

	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   1,
			Hash:     "h1",
			PrevHash: "h0",
			Time:     123,
		}); err != nil {
			return err
		}

		pos, err := tx.NextOrchardCommitmentPosition(ctx)
		if err != nil {
			return err
		}
		if pos != 0 {
			t.Fatalf("expected next position 0, got %d", pos)
		}

		if err := tx.InsertOrchardAction(ctx, store.OrchardAction{
			Height:          1,
			TxID:            "tx1",
			ActionIndex:     0,
			ActionNullifier: "nf_spend_1",
			CMX:             "cmx1",
			EphemeralKey:    "epk1",
			EncCiphertext:   "ct1",
		}); err != nil {
			return err
		}

		if err := tx.InsertOrchardCommitment(ctx, store.OrchardCommitment{
			Position:    0,
			Height:      1,
			TxID:        "tx1",
			ActionIndex: 0,
			CMX:         "cmx1",
		}); err != nil {
			return err
		}

		p0 := int64(0)
		if _, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "tx1",
			ActionIndex:      0,
			Height:           1,
			Position:         &p0,
			DiversifierIndex: 7,
			RecipientAddress: "j1test",
			ValueZat:         10,
			NoteNullifier:    "nf_note_1",
		}); err != nil {
			return err
		}

		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     "deposit",
			WalletID: "hot",
			Height:   1,
			Payload:  payload,
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	tip, ok, err := st.Tip(ctx)
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if !ok || tip.Height != 1 || tip.Hash != "h1" {
		t.Fatalf("unexpected tip: ok=%v tip=%+v", ok, tip)
	}

	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   2,
			Hash:     "h2",
			PrevHash: "h1",
			Time:     124,
		}); err != nil {
			return err
		}
		if _, err := tx.MarkNotesSpent(ctx, 2, "tx2", []string{"nf_note_1"}); err != nil {
			return err
		}
		if _, err := tx.ConfirmNotes(ctx, 2, 1); err != nil {
			return err
		}
		if _, err := tx.ConfirmSpends(ctx, 2, 2); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("WithTx spend: %v", err)
	}

	unspent, err := st.ListWalletNotes(ctx, "hot", true, 100)
	if err != nil {
		t.Fatalf("ListWalletNotes(unspent): %v", err)
	}
	if len(unspent) != 0 {
		t.Fatalf("expected 0 unspent notes, got %d", len(unspent))
	}

	allNotes, err := st.ListWalletNotes(ctx, "hot", false, 100)
	if err != nil {
		t.Fatalf("ListWalletNotes(all): %v", err)
	}
	if len(allNotes) != 1 || allNotes[0].SpentHeight == nil || *allNotes[0].SpentHeight != 2 || allNotes[0].ConfirmedHeight == nil || *allNotes[0].ConfirmedHeight != 2 || allNotes[0].SpentConfirmedHeight == nil || *allNotes[0].SpentConfirmedHeight != 2 {
		t.Fatalf("expected 1 spent note, got %+v", allNotes)
	}

	if err := st.RollbackToHeight(ctx, 1); err != nil {
		t.Fatalf("RollbackToHeight(1): %v", err)
	}

	unspent, err = st.ListWalletNotes(ctx, "hot", true, 100)
	if err != nil {
		t.Fatalf("ListWalletNotes(unspent after rollback): %v", err)
	}
	if len(unspent) != 1 || unspent[0].SpentHeight != nil || unspent[0].ConfirmedHeight != nil || unspent[0].SpentConfirmedHeight != nil {
		t.Fatalf("expected 1 unspent note after rollback, got %+v", unspent)
	}

	eventsAfterRollback, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListWalletEvents(after rollback): %v", err)
	}
	var sawDepositUnconfirmed, sawSpendOrphaned bool
	for _, e := range eventsAfterRollback {
		if e.Kind == "DepositUnconfirmed" {
			sawDepositUnconfirmed = true
		}
		if e.Kind == "SpendOrphaned" {
			sawSpendOrphaned = true
		}
	}
	if !sawDepositUnconfirmed || !sawSpendOrphaned {
		t.Fatalf("expected DepositUnconfirmed and SpendOrphaned after rollback, got %+v", eventsAfterRollback)
	}

	if err := st.RollbackToHeight(ctx, 0); err != nil {
		t.Fatalf("RollbackToHeight(0): %v", err)
	}

	notesAfter, err := st.ListWalletNotes(ctx, "hot", false, 100)
	if err != nil {
		t.Fatalf("ListWalletNotes(after delete): %v", err)
	}
	if len(notesAfter) != 0 {
		t.Fatalf("expected 0 notes after rollback to 0, got %d", len(notesAfter))
	}

	events, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListWalletEvents: %v", err)
	}
	foundOrphaned := false
	for _, e := range events {
		if e.Kind == "DepositOrphaned" {
			foundOrphaned = true
			break
		}
	}
	if !foundOrphaned {
		t.Fatalf("expected DepositOrphaned after rollback to 0, got %+v", events)
	}

	commitments, err := st.ListOrchardCommitmentsUpToHeight(ctx, 100)
	if err != nil {
		t.Fatalf("ListOrchardCommitmentsUpToHeight: %v", err)
	}
	if len(commitments) != 0 {
		t.Fatalf("expected 0 commitments after rollback to 0, got %d", len(commitments))
	}
}

func TestStore_RollbackBelowGenesisPreservesIdentityMetadataAndResetsProgress(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := st.EventEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	progress, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	oldGeneration := progress.Generation
	progress.NextHeight, progress.TargetHeight, progress.State = 101, 100, "complete"
	if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "exchange", TxID: "tx1", Height: 1, RecipientAddress: "u", ValueZat: 1, NoteNullifier: "nf1"}); err != nil {
			return err
		}
		return tx.InsertEvent(ctx, store.Event{Kind: "DepositEvent", WalletID: "exchange", Height: 1, Payload: json.RawMessage(`{"txid":"tx1"}`)})
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.RollbackToHeight(ctx, -1); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.EventEpoch(ctx); got == epoch {
		t.Fatalf("event epoch did not rotate after destructive reset: %q", got)
	}
	if tip, ok, err := st.Tip(ctx); err != nil || ok {
		t.Fatalf("chain data survived rollback: tip=%+v ok=%v err=%v", tip, ok, err)
	}
	wallets, err := st.ListWallets(ctx)
	if err != nil || len(wallets) != 1 || wallets[0].WalletID != "exchange" || wallets[0].BirthdayHeight != 50 || wallets[0].UFVKFingerprint != store.UFVKFingerprint("ufvk") {
		t.Fatalf("wallet identity not preserved: %+v err=%v", wallets, err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 50 || progress.NextHeight != 50 || progress.State != "pending" || progress.Generation <= oldGeneration {
		t.Fatalf("progress not safely reset: %+v ok=%v err=%v", progress, ok, err)
	}
	if notes, err := st.ListWalletNotes(ctx, "exchange", false, 100); err != nil || len(notes) != 0 {
		t.Fatalf("notes survived rollback: %+v err=%v", notes, err)
	}
	if events, _, err := st.ListWalletEvents(ctx, "exchange", 0, 100, store.EventFilter{}); err != nil || len(events) != 0 {
		t.Fatalf("events survived rollback: %+v err=%v", events, err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("schema metadata not preserved: %v", err)
	}
}

func TestStore_AddressBalanceTreatsDisabledWalletAsNotFound(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "disabled-balance.db"))
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
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		_, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: "tx", Height: 1, RecipientAddress: "u1", ValueZat: 7, NoteNullifier: "nf"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if balance, err := st.AddressBalance(ctx, "hot", "u1", 1, 1); err != nil || !balance.WalletFound || balance.TotalUnspentZat != 7 {
		t.Fatalf("enabled balance=%+v err=%v", balance, err)
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
	walletBytes, err = json.Marshal(wallet)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.db.Set(keyWallet("hot"), walletBytes, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if balance, err := st.AddressBalance(ctx, "hot", "u1", 1, 1); err != nil || balance.WalletFound || balance.TotalUnspentZat != 0 {
		t.Fatalf("disabled balance=%+v err=%v", balance, err)
	}
}

func TestStore_RollbackRewindsShardCache(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "shard-rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 5, Hash: "h5"}); err != nil {
			return err
		}
		return tx.InsertBlock(ctx, store.Block{Height: 6, Hash: "h6"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyOrchardShardRoot(ctx, store.OrchardShardRoot{Version: 1, ShardIndex: 0, EndPosition: 4095, EndHeight: 5, EndBlockHash: "h5", Root: "r0"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyOrchardShardRoot(ctx, store.OrchardShardRoot{Version: 1, ShardIndex: 1, EndPosition: 8191, EndHeight: 6, EndBlockHash: "h6", Root: "r1"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := st.RollbackToHeight(ctx, 5); err != nil {
		t.Fatal(err)
	}
	roots, err := st.ListOrchardShardRootsByIndexRange(ctx, 0, 10)
	if err != nil || len(roots) != 1 || roots[0].ShardIndex != 0 {
		t.Fatalf("roots=%+v err=%v", roots, err)
	}
	version, next, err := st.OrchardShardBackfillCursor(ctx)
	if err != nil || version != 1 || next != 1 {
		t.Fatalf("cursor version=%d next=%d err=%v", version, next, err)
	}
}
