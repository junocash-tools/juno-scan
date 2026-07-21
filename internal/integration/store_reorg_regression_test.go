//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

func exerciseRollbackWaitsForCanonicalWriter(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 5, Hash: "h5"})
	}); err != nil {
		t.Fatal(err)
	}

	writerLocked := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- st.WithTx(ctx, func(tx store.Tx) error {
			if err := tx.AssertCanonicalBlock(ctx, 5, "h5"); err != nil {
				return err
			}
			close(writerLocked)
			if _, err := tx.InsertNote(ctx, store.Note{
				WalletID: "hot", TxID: strings.Repeat("a", 64), ActionIndex: 0, Height: 5,
				IsInternal: true, RecipientAddress: "u", ValueZat: 1, NoteNullifier: strings.Repeat("b", 64),
			}); err != nil {
				return err
			}
			if err := tx.InsertEvent(ctx, store.Event{Kind: "BackfillDerived", WalletID: "hot", Height: 5, Payload: json.RawMessage(`{"txid":"derived"}`)}); err != nil {
				return err
			}
			<-releaseWriter
			return nil
		})
	}()
	select {
	case <-writerLocked:
	case err := <-writerDone:
		t.Fatalf("canonical writer ended before lock handoff: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	rollbackStarted := make(chan struct{})
	rollbackDone := make(chan error, 1)
	go func() {
		close(rollbackStarted)
		rollbackDone <- st.RollbackToHeight(ctx, 4)
	}()
	<-rollbackStarted
	select {
	case err := <-rollbackDone:
		close(releaseWriter)
		<-writerDone
		t.Fatalf("rollback bypassed canonical block lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(releaseWriter)
	if err := <-writerDone; err != nil {
		t.Fatalf("canonical writer: %v", err)
	}
	if err := <-rollbackDone; err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, found, err := st.HashAtHeight(ctx, 5); err != nil || found {
		t.Fatalf("rolled-back block found=%v err=%v", found, err)
	}
	if notes, err := st.ListWalletNotes(ctx, "hot", false, 100); err != nil || len(notes) != 0 {
		t.Fatalf("derived notes survived rollback: %+v err=%v", notes, err)
	}
	if got, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{}); err != nil || len(got) != 0 {
		t.Fatalf("derived events survived rollback: %+v err=%v", got, err)
	}
}

func exerciseOutgoingExpiryRollbackReplay(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	seen := time.Unix(100, 0).UTC()
	expiryHeight := int64(5)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 5, Hash: "h5"}); err != nil {
			return err
		}
		if err := tx.InsertBlock(ctx, store.Block{Height: 6, Hash: "h6"}); err != nil {
			return err
		}
		_, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
			WalletID: "hot", TxID: strings.Repeat("c", 64), ActionIndex: 0,
			MempoolSeenAt: &seen, TxExpiryHeight: &expiryHeight, RecipientAddress: "u", ValueZat: 1, OvkScope: "external",
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expire := func() error {
		return st.WithTx(ctx, func(tx store.Tx) error {
			expired, err := tx.ExpireOutgoingOutputs(ctx, 6, time.Now().UTC())
			if err != nil {
				return err
			}
			if len(expired) != 1 {
				t.Fatalf("expired outputs=%d want 1", len(expired))
			}
			return tx.InsertEvent(ctx, store.Event{Kind: events.KindOutgoingOutputExpired, WalletID: "hot", Height: 6, Payload: json.RawMessage(`{"txid":"` + strings.Repeat("c", 64) + `","action_index":0}`)})
		})
	}
	if err := expire(); err != nil {
		t.Fatal(err)
	}
	if err := st.RollbackToHeight(ctx, 5); err != nil {
		t.Fatal(err)
	}
	outputs, err := st.ListOutgoingOutputsByTxID(ctx, "hot", strings.Repeat("c", 64))
	if err != nil || len(outputs) != 1 || outputs[0].ExpiredAt != nil {
		t.Fatalf("output did not unexpire: %+v err=%v", outputs, err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 6, Hash: "h6b"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := expire(); err != nil {
		t.Fatalf("expiry replay: %v", err)
	}
	got, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{Kinds: []string{events.KindOutgoingOutputExpired}})
	if err != nil || len(got) != 1 || got[0].Height != 6 {
		t.Fatalf("expiry replay events=%+v err=%v", got, err)
	}
}

type shardRollbackStore interface {
	ApplyOrchardShardRoot(context.Context, store.OrchardShardRoot, int64) error
	ListOrchardShardRootsByIndexRange(context.Context, int64, int) ([]store.OrchardShardRoot, error)
	OrchardShardBackfillCursor(context.Context) (int32, int64, error)
}

func exerciseShardCacheRollback(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()
	shards, ok := st.(shardRollbackStore)
	if !ok {
		t.Fatal("store lacks shard cache interface")
	}
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
	if err := shards.ApplyOrchardShardRoot(ctx, store.OrchardShardRoot{Version: 1, ShardIndex: 0, EndPosition: 4095, EndHeight: 5, EndBlockHash: "h5", Root: "r0"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := shards.ApplyOrchardShardRoot(ctx, store.OrchardShardRoot{Version: 1, ShardIndex: 1, EndPosition: 8191, EndHeight: 6, EndBlockHash: "h6", Root: "r1"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := st.RollbackToHeight(ctx, 5); err != nil {
		t.Fatal(err)
	}
	roots, err := shards.ListOrchardShardRootsByIndexRange(ctx, 0, 10)
	if err != nil || len(roots) != 1 || roots[0].ShardIndex != 0 {
		t.Fatalf("shard roots after rollback=%+v err=%v", roots, err)
	}
	version, next, err := shards.OrchardShardBackfillCursor(ctx)
	if err != nil || version != 1 || next != 1 {
		t.Fatalf("shard cursor version=%d next=%d err=%v", version, next, err)
	}
}
