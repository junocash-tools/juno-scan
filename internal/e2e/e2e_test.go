//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/testutil"
)

func TestE2E_ScannerAPI_DepositEvent(t *testing.T) {
	dbURL := os.Getenv("JUNO_SCAN_TEST_DB_URL")
	if dbURL == "" {
		t.Skip("set JUNO_SCAN_TEST_DB_URL to run e2e tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pg, err := testutil.OpenTestPostgres(ctx, dbURL)
	if err != nil {
		t.Fatalf("OpenTestPostgres: %v", err)
	}
	defer func() { _ = pg.Close(context.Background()) }()

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

	port := mustFreePort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	bin := filepath.Join("..", "..", "bin", "juno-scan")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("missing binary: %v", err)
	}
	cmd := exec.CommandContext(ctx, bin,
		"-listen", fmt.Sprintf("127.0.0.1:%d", port),
		"-db-url", dbURL,
		"-db-schema", pg.Schema,
		"-rpc-url", jd.RPCURL,
		"-rpc-user", jd.RPCUser,
		"-rpc-pass", jd.RPCPassword,
		"-ua-hrp", uaHRP,
		"-poll-interval", "100ms",
	)
	cmd.Env = append(os.Environ(), "JUNO_TEST_LOG="+os.Getenv("JUNO_TEST_LOG"))
	if os.Getenv("JUNO_TEST_LOG") != "" {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start juno-scan: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	mustWaitHTTP(t, ctx, baseURL+"/v1/health")

	// Add wallet via API.
	body, _ := json.Marshal(map[string]any{
		"wallet_id": "hot",
		"ufvk":      ufvk,
	})
	resp, err := http.Post(baseURL+"/v1/wallets", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/wallets: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets status=%d", resp.StatusCode)
	}

	// Produce a shielded deposit to the wallet's unified address.
	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	mustWaitForDepositEvent(t, ctx, baseURL, "hot")
}

func mustWaitHTTP(t *testing.T, ctx context.Context, url string) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", url)
}

func mustWaitForDepositEvent(t *testing.T, ctx context.Context, baseURL, walletID string) {
	t.Helper()

	client := &http.Client{Timeout: 5 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/wallets/"+walletID+"/events?cursor=0&limit=50", nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var payload struct {
				Events []struct {
					Kind string `json:"kind"`
				} `json:"events"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&payload)
			_ = resp.Body.Close()
			for _, e := range payload.Events {
				if e.Kind == "deposit" {
					return
				}
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("deposit event not found via API")
}

func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
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
	mustRunJSON(t, jd.CLICommand(ctx, "z_getaddressforaccount", fmt.Sprint(acc.Account)), &addrResp)
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
