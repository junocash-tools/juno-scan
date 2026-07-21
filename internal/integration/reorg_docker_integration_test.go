//go:build integration && docker

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScannerRollsBackInvalidatedTipWithoutReplacementBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		t.Fatalf("start junocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "reorg.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	sc, err := scanner.New(st, rpc, "jregtest", 100*time.Millisecond, 100, "")
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	scannerErr := make(chan error, 1)
	go func() { scannerErr <- sc.Run(runCtx) }()
	defer func() {
		runCancel()
		select {
		case <-scannerErr:
		case <-time.After(5 * time.Second):
		}
	}()

	mustRun(t, jd.CLICommand(ctx, "generate", "3"))
	originalHeight, originalHash := waitForExactScannerTip(t, ctx, st, rpc, scannerErr)
	if originalHeight < 1 {
		t.Fatalf("original height=%d want at least 1", originalHeight)
	}

	mustRun(t, jd.CLICommand(ctx, "invalidateblock", originalHash))
	rolledBackHeight, rolledBackHash := waitForExactScannerTip(t, ctx, st, rpc, scannerErr)
	if rolledBackHeight != originalHeight-1 {
		t.Fatalf("rollback height=%d want %d", rolledBackHeight, originalHeight-1)
	}

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	replacementHeight, replacementHash := waitForExactScannerTip(t, ctx, st, rpc, scannerErr)
	if replacementHeight != originalHeight || replacementHash == originalHash || rolledBackHash == replacementHash {
		t.Fatalf("replacement height=%d hash=%s original=%s ancestor=%s", replacementHeight, replacementHash, originalHash, rolledBackHash)
	}
}

func waitForExactScannerTip(
	t *testing.T,
	ctx context.Context,
	st store.Store,
	rpc *sdkjunocashd.Client,
	scannerErr <-chan error,
) (int64, string) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-scannerErr:
			t.Fatalf("scanner stopped before reaching canonical tip: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for exact scanner tip: %v", ctx.Err())
		case <-ticker.C:
			chainHeight, err := rpc.GetBlockCount(ctx)
			if err != nil {
				continue
			}
			chainHash, err := rpc.GetBlockHash(ctx, chainHeight)
			if err != nil {
				continue
			}
			tip, ok, err := st.Tip(ctx)
			if err == nil && ok && tip.Height == chainHeight && tip.Hash == chainHash {
				return tip.Height, tip.Hash
			}
		}
	}
}
