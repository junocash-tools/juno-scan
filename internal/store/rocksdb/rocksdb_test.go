package rocksdb

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
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
		if err := tx.InsertNote(ctx, store.Note{
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
		if err := tx.MarkNotesSpent(ctx, 2, "tx2", []string{"nf_note_1"}); err != nil {
			return err
		}
		_, err := tx.ConfirmNotes(ctx, 2, 1)
		return err
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
	if len(allNotes) != 1 || allNotes[0].SpentHeight == nil || *allNotes[0].SpentHeight != 2 || allNotes[0].ConfirmedHeight == nil || *allNotes[0].ConfirmedHeight != 2 {
		t.Fatalf("expected 1 spent note, got %+v", allNotes)
	}

	if err := st.RollbackToHeight(ctx, 1); err != nil {
		t.Fatalf("RollbackToHeight(1): %v", err)
	}

	unspent, err = st.ListWalletNotes(ctx, "hot", true, 100)
	if err != nil {
		t.Fatalf("ListWalletNotes(unspent after rollback): %v", err)
	}
	if len(unspent) != 1 || unspent[0].SpentHeight != nil || unspent[0].ConfirmedHeight != nil {
		t.Fatalf("expected 1 unspent note after rollback, got %+v", unspent)
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

	events, _, err := st.ListWalletEvents(ctx, "hot", 0, 100)
	if err != nil {
		t.Fatalf("ListWalletEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events after rollback to 0, got %d", len(events))
	}

	commitments, err := st.ListOrchardCommitmentsUpToHeight(ctx, 100)
	if err != nil {
		t.Fatalf("ListOrchardCommitmentsUpToHeight: %v", err)
	}
	if len(commitments) != 0 {
		t.Fatalf("expected 0 commitments after rollback to 0, got %d", len(commitments))
	}
}
