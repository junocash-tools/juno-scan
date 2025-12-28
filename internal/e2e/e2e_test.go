//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/testutil"
)

func TestE2E_ScannerAPI_DepositEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

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

	port := mustFreePort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	dbPath := filepath.Join(t.TempDir(), "db")

	bin := filepath.Join("..", "..", "bin", "juno-scan")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("missing binary: %v", err)
	}
	cmd := exec.CommandContext(ctx, bin,
		"-listen", fmt.Sprintf("127.0.0.1:%d", port),
		"-db-driver", "rocksdb",
		"-db-path", dbPath,
		"-rpc-url", jd.RPCURL,
		"-rpc-user", jd.RPCUser,
		"-rpc-pass", jd.RPCPassword,
		"-ua-hrp", uaHRP,
		"-poll-interval", "100ms",
		"-confirmations", "2",
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

	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositEvent")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositConfirmed")

	notes := mustGetNotes(t, ctx, baseURL, "hot", false)
	if len(notes) == 0 || notes[0].Position == nil {
		t.Fatalf("expected at least 1 note with position")
	}
	if *notes[0].Position < 0 {
		t.Fatalf("invalid note position: %d", *notes[0].Position)
	}

	wit := mustGetWitness(t, ctx, baseURL, []uint32{uint32(*notes[0].Position)})
	if wit.Root == "" {
		t.Fatalf("missing witness root")
	}
	if len(wit.Paths) != 1 {
		t.Fatalf("expected 1 witness path, got %d", len(wit.Paths))
	}
	if len(wit.Paths[0].AuthPath) != 32 {
		t.Fatalf("expected auth_path length 32, got %d", len(wit.Paths[0].AuthPath))
	}

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend.
	toAddr := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendMany(t, ctx, jd, addr, toAddr, "0.01")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitForEventKind(t, ctx, baseURL, "hot", "SpendEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitForEventKind(t, ctx, baseURL, "hot", "SpendConfirmed")

	allNotes := mustGetNotes(t, ctx, baseURL, "hot", true)
	foundSpent := false
	for _, n := range allNotes {
		if n.SpentHeight != nil {
			foundSpent = true
			break
		}
	}
	if !foundSpent {
		t.Fatalf("expected at least 1 spent note")
	}
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

func mustWaitForEventKind(t *testing.T, ctx context.Context, baseURL, walletID, kind string) {
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
				if e.Kind == kind {
					return
				}
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("%s not found via API", kind)
}

type apiNote struct {
	TxID        string `json:"txid"`
	ActionIndex int32  `json:"action_index"`
	Position    *int64 `json:"position,omitempty"`
	SpentHeight *int64 `json:"spent_height,omitempty"`
}

func mustGetNotes(t *testing.T, ctx context.Context, baseURL, walletID string, spent bool) []apiNote {
	t.Helper()

	url := fmt.Sprintf("%s/v1/wallets/%s/notes?spent=%t", baseURL, walletID, spent)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET /v1/wallets/%s/notes: %v", walletID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/wallets/%s/notes status=%d", walletID, resp.StatusCode)
	}
	var out struct {
		Notes []apiNote `json:"notes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode notes: %v", err)
	}
	return out.Notes
}

type witnessResponse struct {
	Status       string `json:"status"`
	AnchorHeight int64  `json:"anchor_height"`
	Root         string `json:"root"`
	Paths        []struct {
		Position uint32   `json:"position"`
		AuthPath []string `json:"auth_path"`
	} `json:"paths"`
}

func mustGetWitness(t *testing.T, ctx context.Context, baseURL string, positions []uint32) witnessResponse {
	t.Helper()

	body, _ := json.Marshal(map[string]any{
		"positions": positions,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/orchard/witness", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST /v1/orchard/witness: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /v1/orchard/witness status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out witnessResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode witness: %v", err)
	}
	if out.Status != "ok" {
		t.Fatalf("unexpected witness status=%q", out.Status)
	}
	if out.AnchorHeight < 0 {
		t.Fatalf("invalid anchor_height=%d", out.AnchorHeight)
	}
	return out
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

func mustCreateUnifiedAddress(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) (addr string) {
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
	return addrResp.Address
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

	out := mustRun(t, jd.CLICommand(ctx, "z_shieldcoinbase", fromAddr, toAddr))

	var resp struct {
		OpID string `json:"opid"`
	}
	if err := json.Unmarshal(out, &resp); err == nil && resp.OpID != "" {
		return resp.OpID
	}
	opid := strings.TrimSpace(string(out))
	if opid == "" {
		t.Fatalf("missing opid")
	}
	return opid
}

func mustSendMany(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr, toAddr string, amount string) string {
	t.Helper()

	recipients := `[{"address":"` + toAddr + `","amount":` + amount + `}]`
	out := mustRun(t, jd.CLICommand(ctx, "z_sendmany", fromAddr, recipients, "1"))

	var resp struct {
		OpID string `json:"opid"`
	}
	if err := json.Unmarshal(out, &resp); err == nil && resp.OpID != "" {
		return resp.OpID
	}
	opid := strings.TrimSpace(string(out))
	if opid == "" {
		t.Fatalf("missing opid")
	}
	return opid
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
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}
		if err := json.Unmarshal(out, &res); err == nil && len(res) > 0 {
			switch res[0].Status {
			case "success":
				return
			case "failed":
				msg := ""
				if res[0].Error != nil {
					msg = res[0].Error.Message
				}
				t.Fatalf("operation failed: %s (%s)", opid, msg)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("operation did not succeed: %s", opid)
}

func mustWaitOrchardBalanceForViewingKey(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, ufvk string, minconf int) int64 {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type pool struct {
		ValueZat int64 `json:"valueZat"`
	}
	type resp struct {
		Pools map[string]pool `json:"pools"`
	}

	for time.Now().Before(deadline) {
		out := mustRun(t, jd.CLICommand(ctx, "z_getbalanceforviewingkey", ufvk, strconv.FormatInt(int64(minconf), 10)))
		var r resp
		if err := json.Unmarshal(out, &r); err == nil {
			if p, ok := r.Pools["orchard"]; ok && p.ValueZat > 0 {
				return p.ValueZat
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("orchard balance not available for minconf=%d", minconf)
	return 0
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
