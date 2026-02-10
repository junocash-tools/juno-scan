//go:build integration && docker && mysql

package integration_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_MempoolSpendExpires_MySQL(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
	defer cleanup()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	addr, ufvk := mustCreateWalletAndUFVK(t, ctx, jd)
	uaHRP := strings.SplitN(addr, "1", 2)[0]

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := st.UpsertWallet(ctx, "hot", ufvk); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	sc, err := scanner.New(st, rpc, uaHRP, 100*time.Millisecond, 2, "")
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	scErrCh := make(chan error, 1)
	go func() { scErrCh <- sc.Run(runCtx) }()
	defer func() {
		runCancel()
		select {
		case <-scErrCh:
		case <-time.After(5 * time.Second):
		}
	}()

	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	waitForEventKind(t, ctx, st, "hot", "DepositEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	to1 := mustCreateUnifiedAddress(t, ctx, jd)
	to2 := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendManyWithFee(t, ctx, jd, addr, []string{to1, to2}, []string{"0.01", "0.01"}, "1", "0.0001")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)

	expiryHeight := waitForPendingSpendWithExpiryHeight(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventMempoolWithExpiryHeight(t, ctx, st, "hot", spendTxID, expiryHeight)

	tip, err := rpc.GetBlockCount(ctx)
	if err != nil {
		t.Fatalf("getblockcount: %v", err)
	}
	blocksToMine := (expiryHeight + 1) - tip
	if blocksToMine < 1 {
		blocksToMine = 1
	}
	mustRun(t, jd.CLICommand(ctx, "generate", strconv.FormatInt(blocksToMine, 10)))

	waitForOutgoingOutputExpiredEvent(t, ctx, st, "hot", spendTxID, expiryHeight)
	waitForPendingSpendCleared(t, ctx, st, "hot", spendTxID)
}
