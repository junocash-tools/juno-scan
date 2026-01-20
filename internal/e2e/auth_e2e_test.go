//go:build e2e

package e2e_test

import (
	"context"
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

func TestE2E_ScannerAPI_BearerAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	addr, _ := mustCreateWalletAndUFVK(t, ctx, jd)
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
		"-api-bearer-token", "secret",
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

	// Without auth, we expect a 401 from /v1/health once the server is up.
	mustWaitHTTPStatus(t, ctx, baseURL+"/v1/health", http.StatusUnauthorized, nil)

	// With auth, we expect a 200.
	mustWaitHTTPStatus(t, ctx, baseURL+"/v1/health", http.StatusOK, map[string]string{
		"Authorization": "Bearer secret",
	})
}

func mustWaitHTTPStatus(t *testing.T, ctx context.Context, url string, wantStatus int, headers map[string]string) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == wantStatus {
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s status=%d", url, wantStatus)
}
