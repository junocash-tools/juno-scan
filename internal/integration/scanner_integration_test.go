//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/db/migrate"
	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_DepositDetected(t *testing.T) {
	dbURL := os.Getenv("JUNO_SCAN_TEST_DB_URL")
	if dbURL == "" {
		t.Skip("set JUNO_SCAN_TEST_DB_URL to run integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg, err := testutil.OpenTestPostgres(ctx, dbURL)
	if err != nil {
		t.Fatalf("OpenTestPostgres: %v", err)
	}
	defer func() { _ = pg.Close(context.Background()) }()

	if err := migrate.Apply(ctx, pg.Pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	addr, ufvk := mustCreateWalletAndUFVK(t, ctx, jd)
	uaHRP := strings.SplitN(addr, "1", 2)[0]

	if _, err := pg.Pool.Exec(ctx, `INSERT INTO wallets (wallet_id, ufvk) VALUES ($1, $2)`, "hot", ufvk); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}

	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	sc, err := scanner.New(pg.Pool, rpc, uaHRP, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}

	go func() { _ = sc.Run(ctx) }()

	mustRun(t, jd.CLICommand(ctx, "generate", "101"))

	fromAddr := mustCoinbaseAddress(t, ctx, jd)

	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	waitForEvent(t, ctx, pg, "hot")
}

func mustCreateWalletAndUFVK(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) (addr string, ufvk string) {
	t.Helper()

	var acc struct {
		Account int `json:"account"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getnewaccount"), &acc)

	var addrResp struct {
		Address string `json:"address"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getaddressforaccount", strconvI(acc.Account)), &addrResp)
	if addrResp.Address == "" {
		t.Fatalf("missing address")
	}

	out := mustRun(t, jd.CLICommand(ctx, "z_exportviewingkey", addrResp.Address))
	ufvk = strings.TrimSpace(string(out))
	if ufvk == "" {
		t.Fatalf("missing ufvk")
	}
	return addrResp.Address, ufvk
}

func mustCoinbaseAddress(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) string {
	t.Helper()

	var utxos []struct {
		Address string `json:"address"`
	}
	out := mustRun(t, jd.CLICommand(ctx, "listunspent", "1", "9999999"))
	if err := json.Unmarshal(out, &utxos); err != nil {
		t.Fatalf("listunspent json: %v\n%s", err, string(out))
	}
	if len(utxos) == 0 || utxos[0].Address == "" {
		t.Fatalf("no utxos found")
	}
	return utxos[0].Address
}

func mustShieldCoinbase(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr, toAddr string) string {
	t.Helper()

	var resp struct {
		OpID string `json:"opid"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_shieldcoinbase", fromAddr, toAddr), &resp)
	if resp.OpID == "" {
		t.Fatalf("missing opid")
	}
	return resp.OpID
}

func mustWaitOpSuccess(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, opid string) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		out := mustRun(t, jd.CLICommand(ctx, "z_getoperationresult", `["`+opid+`"]`))
		var res []struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(out, &res); err == nil && len(res) > 0 && res[0].Status == "success" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("operation did not succeed: %s", opid)
}

func waitForEvent(t *testing.T, ctx context.Context, pg *testutil.TestPostgres, walletID string) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		var count int64
		if err := pg.Pool.QueryRow(ctx, `SELECT COUNT(1) FROM events WHERE wallet_id=$1 AND kind='deposit'`, walletID).Scan(&count); err == nil && count > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("deposit event not found")
}

func mustRun(t *testing.T, cmd *exec.Cmd) []byte {
	t.Helper()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(cmd.Args, " "), err, string(out))
	}
	return out
}

func mustRunJSON(t *testing.T, cmd *exec.Cmd, out any) {
	t.Helper()
	b := mustRun(t, cmd)
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("%s: unmarshal: %v\n%s", strings.Join(cmd.Args, " "), err, string(b))
	}
}

func strconvI(n int) string {
	return strconv.FormatInt(int64(n), 10)
}
