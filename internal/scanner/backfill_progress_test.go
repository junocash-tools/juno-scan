package scanner

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestPostCommitReconciliationClosesBackfillCompletionCrossing(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "progress-crossing.db"))
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
	progress, _, err := st.WalletBackfillStatus(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	progress.NextHeight, progress.TargetHeight, progress.State = 1, 1, "running"
	if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
		t.Fatal(err)
	}

	// The live block transaction observes running progress and cannot advance it.
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		return tx.AdvanceCompleteWalletBackfillProgress(ctx, 1)
	}); err != nil {
		t.Fatal(err)
	}
	// The API completion CAS crosses the block commit before the post-commit
	// reconciliation executes.
	progress, _, err = st.WalletBackfillStatus(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	progress.State = "complete"
	if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
		t.Fatal(err)
	}
	if err := reconcileCompleteWalletBackfillProgress(ctx, st, 1); err != nil {
		t.Fatal(err)
	}
	progress, _, err = st.WalletBackfillStatus(ctx, "hot")
	if err != nil || progress.State != "complete" || progress.NextHeight != 2 || progress.TargetHeight != 1 {
		t.Fatalf("post-commit progress=%+v err=%v", progress, err)
	}
}
