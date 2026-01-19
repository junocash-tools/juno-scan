//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/testutil"
)

func TestE2E_ScannerAPI_BackfillWallet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	// Produce a shielded deposit before adding the wallet.
	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "2"))

	chainHeight := mustBlockCount(t, ctx, jd)
	mustWaitScannedHeight(t, ctx, baseURL, chainHeight)

	// Add wallet via API after the deposit already happened.
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

	// Confirm there's no historic DepositEvent before backfill.
	if hasEventKind(t, ctx, baseURL, "hot", "DepositEvent") {
		t.Fatalf("unexpected DepositEvent before backfill")
	}

	// Backfill the wallet history (single batch).
	backfillBody, _ := json.Marshal(map[string]any{
		"from_height": 0,
		"batch_size":  10_000,
	})
	bfResp, err := http.Post(baseURL+"/v1/wallets/hot/backfill", "application/json", bytes.NewReader(backfillBody))
	if err != nil {
		t.Fatalf("POST /v1/wallets/hot/backfill: %v", err)
	}
	_ = bfResp.Body.Close()
	if bfResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets/hot/backfill status=%d", bfResp.StatusCode)
	}

	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositEvent")
	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositConfirmed")
}

func hasEventKind(t *testing.T, ctx context.Context, baseURL, walletID, kind string) bool {
	t.Helper()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/wallets/"+walletID+"/events?cursor=0&limit=100", nil)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil || resp == nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var payload struct {
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	for _, e := range payload.Events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}
