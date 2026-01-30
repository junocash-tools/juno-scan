package rocksdb

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func TestStore_RollbackOutgoingOutputs(t *testing.T) {
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

	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   1,
			Hash:     "h1",
			PrevHash: "h0",
			Time:     123,
		}); err != nil {
			return err
		}
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   2,
			Hash:     "h2",
			PrevHash: "h1",
			Time:     124,
		}); err != nil {
			return err
		}

		h1 := int64(1)
		if _, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
			WalletID: "hot",
			TxID:     "tx1",

			ActionIndex: 0,

			MinedHeight: &h1,

			RecipientAddress: "j1recipient",
			ValueZat:         123,

			OvkScope: "external",
		}); err != nil {
			return err
		}

		h2 := int64(2)
		if _, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
			WalletID: "hot",
			TxID:     "tx2",

			ActionIndex: 0,

			MinedHeight: &h2,

			RecipientAddress: "j1recipient",
			ValueZat:         456,

			OvkScope: "external",
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	if err := st.WithTx(ctx, func(tx store.Tx) error {
		_, err := tx.ConfirmOutgoingOutputs(ctx, 2, 1)
		return err
	}); err != nil {
		t.Fatalf("WithTx ConfirmOutgoingOutputs: %v", err)
	}

	if err := st.RollbackToHeight(ctx, 1); err != nil {
		t.Fatalf("RollbackToHeight(1): %v", err)
	}

	outs1, err := st.ListOutgoingOutputsByTxID(ctx, "hot", "tx1")
	if err != nil {
		t.Fatalf("ListOutgoingOutputsByTxID(tx1): %v", err)
	}
	if len(outs1) != 1 || outs1[0].MinedHeight == nil || *outs1[0].MinedHeight != 1 || outs1[0].ConfirmedHeight != nil {
		t.Fatalf("expected tx1 output mined at 1 and unconfirmed, got %+v", outs1)
	}

	outs2, err := st.ListOutgoingOutputsByTxID(ctx, "hot", "tx2")
	if err != nil {
		t.Fatalf("ListOutgoingOutputsByTxID(tx2): %v", err)
	}
	if len(outs2) != 0 {
		t.Fatalf("expected tx2 output to be deleted after rollback, got %+v", outs2)
	}

	evs, _, err := st.ListWalletEvents(ctx, "hot", 0, 1000, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListWalletEvents: %v", err)
	}

	var sawUnconfirmed, sawOrphaned bool
	for _, e := range evs {
		switch e.Kind {
		case "OutgoingOutputUnconfirmed":
			var payload struct {
				TxID string `json:"txid"`
			}
			_ = json.Unmarshal(e.Payload, &payload)
			if payload.TxID == "tx1" {
				sawUnconfirmed = true
			}
		case "OutgoingOutputOrphaned":
			var payload struct {
				TxID string `json:"txid"`
			}
			_ = json.Unmarshal(e.Payload, &payload)
			if payload.TxID == "tx2" {
				sawOrphaned = true
			}
		}
	}
	if !sawUnconfirmed || !sawOrphaned {
		t.Fatalf("expected OutgoingOutputUnconfirmed(tx1) and OutgoingOutputOrphaned(tx2), got %+v", evs)
	}
}

