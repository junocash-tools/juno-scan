//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store/postgres"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/jackc/pgx/v5"
)

func TestScanner_DepositDetected_Postgres(t *testing.T) {
	dsn := os.Getenv("JUNO_TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	schema := fmt.Sprintf("junoscan_test_%d", time.Now().UnixNano())
	st, err := postgres.Open(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	defer dropPostgresSchema(t, ctx, dsn, schema)

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

	deposit := waitForEventKind(t, ctx, st, "hot", "DepositEvent")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	confirmed := waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	if mustTxIDFromPayload(t, confirmed.Payload) != mustTxIDFromPayload(t, deposit.Payload) {
		t.Fatalf("confirmed txid mismatch")
	}

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend.
	toAddr := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendMany(t, ctx, jd, addr, toAddr, "0.01")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)
	waitForPendingSpend(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventState(t, ctx, st, "hot", spendTxID, "mempool")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendEvent")
	waitForOutgoingOutputEventState(t, ctx, st, "hot", spendTxID, "confirmed")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendConfirmed")
	waitForEventKind(t, ctx, st, "hot", "OutgoingOutputConfirmed")

	notesAll, err := st.ListWalletNotes(ctx, "hot", false, 1000)
	if err != nil {
		t.Fatalf("ListWalletNotes(all): %v", err)
	}
	foundSpent := false
	for _, n := range notesAll {
		if n.SpentTxID != nil && strings.TrimSpace(*n.SpentTxID) == spendTxID {
			foundSpent = true
			if n.PendingSpentTxID != nil {
				t.Fatalf("pending_spent_txid not cleared")
			}
			if n.PendingSpentAt != nil {
				t.Fatalf("pending_spent_at not cleared")
			}
			if n.PendingSpentExpiryHeight != nil {
				t.Fatalf("pending_spent_expiry_height not cleared")
			}
		}
	}
	if !foundSpent {
		t.Fatalf("spent note not found")
	}
}

func dropPostgresSchema(t *testing.T, ctx context.Context, dsn string, schema string) {
	t.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	_, _ = conn.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
}
