//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_DepositDetected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

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
	sc, err := scanner.New(st, rpc, uaHRP, 100*time.Millisecond, 2)
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}

	go func() { _ = sc.Run(ctx) }()

	mustRun(t, jd.CLICommand(ctx, "generate", "101"))

	fromAddr := mustCoinbaseAddress(t, ctx, jd)

	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	deposit := waitForEventKind(t, ctx, st, "hot", "DepositEvent")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	confirmed := waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	var confirmedPayload struct {
		TxID                 string `json:"txid"`
		RequiredConfirmations int64  `json:"required_confirmations"`
		ConfirmedHeight      int64  `json:"confirmed_height"`
		Status               struct {
			Confirmations int64 `json:"confirmations"`
		} `json:"status"`
	}
	if err := json.Unmarshal(confirmed.Payload, &confirmedPayload); err != nil {
		t.Fatalf("unmarshal confirmed payload: %v", err)
	}
	if confirmedPayload.TxID == "" {
		t.Fatalf("missing txid in confirmed payload")
	}
	if confirmedPayload.TxID != mustTxIDFromPayload(t, deposit.Payload) {
		t.Fatalf("confirmed txid mismatch")
	}
	if confirmedPayload.RequiredConfirmations != 2 {
		t.Fatalf("required_confirmations=%d want 2", confirmedPayload.RequiredConfirmations)
	}
	if confirmedPayload.Status.Confirmations != 2 {
		t.Fatalf("status.confirmations=%d want 2", confirmedPayload.Status.Confirmations)
	}
	if confirmedPayload.ConfirmedHeight <= 0 {
		t.Fatalf("invalid confirmed_height=%d", confirmedPayload.ConfirmedHeight)
	}
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

func waitForEventKind(t *testing.T, ctx context.Context, st store.Store, walletID string, kind string) store.Event {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		events, _, err := st.ListWalletEvents(ctx, walletID, 0, 10)
		if err == nil {
			for _, e := range events {
				if e.Kind == kind {
					return e
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s not found", kind)
	return store.Event{}
}

func mustTxIDFromPayload(t *testing.T, payload json.RawMessage) string {
	t.Helper()

	var out struct {
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(payload, &out); err != nil || out.TxID == "" {
		t.Fatalf("missing txid in payload: %v", err)
	}
	return out.TxID
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
