//go:build integration && mysql

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store/mysql"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_DepositDetected_MySQL(t *testing.T) {
	rootDSN := os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN")
	if strings.TrimSpace(rootDSN) == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendConfirmed")
}

func openMySQLTestStore(t *testing.T, ctx context.Context, rootDSN string) (*mysql.Store, func()) {
	t.Helper()

	cfg, err := driver.ParseDSN(rootDSN)
	if err != nil {
		t.Fatalf("parse root dsn: %v", err)
	}
	if cfg.DBName == "" {
		t.Fatalf("root dsn must include a database name (e.g. /mysql)")
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping: %v", err)
	}

	dbName := fmt.Sprintf("junoscan_test_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE DATABASE `"+dbName+"`"); err != nil {
		_ = db.Close()
		t.Fatalf("create database: %v", err)
	}

	cfg2 := *cfg
	cfg2.DBName = dbName
	st, err := mysql.Open(ctx, cfg2.FormatDSN())
	if err != nil {
		_, _ = db.ExecContext(ctx, "DROP DATABASE `"+dbName+"`")
		_ = db.Close()
		t.Fatalf("mysql.Open: %v", err)
	}

	cleanup := func() {
		_ = st.Close()
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE `"+dbName+"`")
		_ = db.Close()
	}

	return st, cleanup
}
