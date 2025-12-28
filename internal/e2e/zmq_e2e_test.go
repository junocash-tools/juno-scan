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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/testutil"
)

func TestE2E_Scanner_ZMQHashblockTriggersScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{EnableZMQHashBlock: true})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	zmqEndpoint := strings.TrimSpace(os.Getenv("JUNO_TEST_ZMQ_HASHBLOCK"))
	if zmqEndpoint == "" {
		zmqEndpoint = jd.ZMQHashBlockEndpoint
	}
	if zmqEndpoint == "" {
		t.Skip("JUNO_TEST_ZMQ_HASHBLOCK not set and node did not advertise ZMQ hashblock endpoint")
	}

	// Prepare spendable funds before starting the scanner.
	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	chainHeight := mustBlockCount(t, ctx, jd)

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
		"-poll-interval", "1h",
		"-confirmations", "1",
		"-zmq-hashblock", zmqEndpoint,
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

	mustWaitScannedHeight(t, ctx, baseURL, chainHeight)

	// Mine a shielded deposit and assert it is detected quickly.
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	fastCtx, fastCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fastCancel()
	mustWaitForEventKind(t, fastCtx, baseURL, "hot", "DepositEvent")
	mustWaitForEventKind(t, fastCtx, baseURL, "hot", "DepositConfirmed")
}

func mustWaitScannedHeight(t *testing.T, ctx context.Context, baseURL string, height int64) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/health", nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var payload struct {
				ScannedHeight *int64 `json:"scanned_height,omitempty"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&payload)
			_ = resp.Body.Close()
			if payload.ScannedHeight != nil && *payload.ScannedHeight >= height {
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for scanned_height >= %d", height)
}

func mustBlockCount(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) int64 {
	t.Helper()
	out := strings.TrimSpace(string(mustRun(t, jd.CLICommand(ctx, "getblockcount"))))
	n, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		t.Fatalf("getblockcount parse: %v (%q)", err, out)
	}
	return n
}
