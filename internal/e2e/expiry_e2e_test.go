//go:build e2e && docker

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestE2E_ScannerAPI_MempoolSpendExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
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

	// Fund.
	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitForEventKind(t, ctx, baseURL, "hot", "DepositConfirmed")

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend with a low fee so the tx enters the mempool but is not mined (see docker test daemon flags).
	to1 := mustCreateUnifiedAddress(t, ctx, jd)
	to2 := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendManyWithFee(t, ctx, jd, addr, []string{to1, to2}, []string{"0.01", "0.01"}, "1", "0.0001")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)

	expiryHeight := mustWaitForPendingSpendExpiryHeightViaAPI(t, ctx, baseURL, "hot", spendTxID)
	mustWaitForOutgoingOutputMempoolWithExpiryHeight(t, ctx, baseURL, "hot", spendTxID, expiryHeight)

	tip := mustBlockCount(t, ctx, jd)
	blocksToMine := (expiryHeight + 1) - tip
	if blocksToMine < 1 {
		blocksToMine = 1
	}
	mustRun(t, jd.CLICommand(ctx, "generate", strconv.FormatInt(blocksToMine, 10)))

	mustWaitForOutgoingOutputExpiredWithExpiryHeight(t, ctx, baseURL, "hot", spendTxID, expiryHeight)
	mustWaitForPendingSpendClearedViaAPI(t, ctx, baseURL, "hot", spendTxID)
}

func mustSendManyWithFee(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr string, toAddrs []string, amounts []string, minconf string, fee string) string {
	t.Helper()
	if len(toAddrs) != len(amounts) || len(toAddrs) == 0 {
		t.Fatalf("toAddrs/amounts mismatch")
	}

	var b strings.Builder
	b.WriteString("[")
	for i := range toAddrs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"address":"`)
		b.WriteString(toAddrs[i])
		b.WriteString(`","amount":`)
		b.WriteString(amounts[i])
		b.WriteString("}")
	}
	b.WriteString("]")

	out := mustRun(t, jd.CLICommand(ctx, "z_sendmany", fromAddr, b.String(), strings.TrimSpace(minconf), strings.TrimSpace(fee)))

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

func mustWaitForPendingSpendExpiryHeightViaAPI(t *testing.T, ctx context.Context, baseURL, walletID, txid string) int64 {
	t.Helper()

	client := &http.Client{Timeout: 10 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type note struct {
		PendingTxID         *string `json:"pending_spent_txid,omitempty"`
		PendingExpiryHeight *int64  `json:"pending_spent_expiry_height,omitempty"`
	}

	for time.Now().Before(deadline) {
		url := fmt.Sprintf("%s/v1/wallets/%s/notes?spent=false&limit=1000", baseURL, walletID)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var out struct {
				Notes []note `json:"notes"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()

			for _, n := range out.Notes {
				if n.PendingTxID == nil || strings.TrimSpace(*n.PendingTxID) != txid {
					continue
				}
				if n.PendingExpiryHeight == nil || *n.PendingExpiryHeight <= 0 {
					t.Fatalf("pending_spent_expiry_height missing for txid=%s", txid)
				}
				return *n.PendingExpiryHeight
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("pending spend not observed via API for txid=%s", txid)
	return 0
}

func mustWaitForPendingSpendClearedViaAPI(t *testing.T, ctx context.Context, baseURL, walletID, txid string) {
	t.Helper()

	client := &http.Client{Timeout: 10 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type note struct {
		PendingTxID *string `json:"pending_spent_txid,omitempty"`
	}

	for time.Now().Before(deadline) {
		url := fmt.Sprintf("%s/v1/wallets/%s/notes?spent=false&limit=1000", baseURL, walletID)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var out struct {
				Notes []note `json:"notes"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()

			stillPending := false
			for _, n := range out.Notes {
				if n.PendingTxID != nil && strings.TrimSpace(*n.PendingTxID) == txid {
					stillPending = true
					break
				}
			}
			if !stillPending {
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("pending spend not cleared via API for txid=%s", txid)
}

func mustWaitForOutgoingOutputMempoolWithExpiryHeight(t *testing.T, ctx context.Context, baseURL, walletID, txid string, wantExpiryHeight int64) {
	t.Helper()

	client := &http.Client{Timeout: 10 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID         string `json:"txid"`
		Height       *int64 `json:"height,omitempty"`
		ExpiryHeight *int64 `json:"expiry_height,omitempty"`
		Status       struct {
			State string `json:"state"`
		} `json:"status"`
	}

	for time.Now().Before(deadline) {
		url := baseURL + "/v1/wallets/" + walletID + "/events?cursor=0&limit=1000&kind=OutgoingOutputEvent&txid=" + txid
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var out struct {
				Events []struct {
					Kind    string          `json:"kind"`
					Height  int64           `json:"height"`
					Payload json.RawMessage `json:"payload"`
				} `json:"events"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()

			for _, e := range out.Events {
				if e.Kind != "OutgoingOutputEvent" {
					t.Fatalf("unexpected kind in filtered response: %s", e.Kind)
				}
				var p payload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					continue
				}
				if p.TxID != txid || p.Status.State != "mempool" {
					continue
				}
				if e.Height != 0 {
					t.Fatalf("expected top-level height=0 for mempool outgoing output, got %d", e.Height)
				}
				if p.Height != nil {
					t.Fatalf("expected no payload.height for mempool outgoing output")
				}
				if p.ExpiryHeight == nil || *p.ExpiryHeight != wantExpiryHeight {
					t.Fatalf("expected payload.expiry_height=%d, got %+v", wantExpiryHeight, p.ExpiryHeight)
				}
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("OutgoingOutputEvent txid=%s state=mempool expiry_height=%d not found via API", txid, wantExpiryHeight)
}

func mustWaitForOutgoingOutputExpiredWithExpiryHeight(t *testing.T, ctx context.Context, baseURL, walletID, txid string, wantExpiryHeight int64) {
	t.Helper()

	client := &http.Client{Timeout: 10 * time.Second}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID         string `json:"txid"`
		Height       *int64 `json:"height,omitempty"`
		ExpiryHeight *int64 `json:"expiry_height,omitempty"`
		Status       struct {
			State string `json:"state"`
		} `json:"status"`
	}

	for time.Now().Before(deadline) {
		url := baseURL + "/v1/wallets/" + walletID + "/events?cursor=0&limit=1000&kind=OutgoingOutputExpired&txid=" + txid
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var out struct {
				Events []struct {
					Kind    string          `json:"kind"`
					Height  int64           `json:"height"`
					Payload json.RawMessage `json:"payload"`
				} `json:"events"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()

			for _, e := range out.Events {
				if e.Kind != "OutgoingOutputExpired" {
					t.Fatalf("unexpected kind in filtered response: %s", e.Kind)
				}
				if e.Height <= wantExpiryHeight {
					continue
				}
				var p payload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					continue
				}
				if p.TxID != txid || p.Status.State != "expired" {
					continue
				}
				if p.Height != nil {
					t.Fatalf("expected no payload.height for expired outgoing output")
				}
				if p.ExpiryHeight == nil || *p.ExpiryHeight != wantExpiryHeight {
					t.Fatalf("expected payload.expiry_height=%d, got %+v", wantExpiryHeight, p.ExpiryHeight)
				}
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("OutgoingOutputExpired txid=%s expiry_height=%d not found via API", txid, wantExpiryHeight)
}
