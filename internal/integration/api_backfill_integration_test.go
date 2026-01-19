//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/api"
	"github.com/Abdullah1738/juno-scan/internal/backfill"
	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestIntegration_API_BackfillWallet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	// Produce a deposit before registering the wallet with the scanner.
	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "2"))

	waitForScannerTip(t, ctx, st, rpc)

	// Start API (with backfill enabled) and register wallet *after* the deposit.
	bf, err := backfill.New(st, rpc, uaHRP, 2)
	if err != nil {
		t.Fatalf("backfill.New: %v", err)
	}
	apiServer, err := api.New(st, api.WithBackfillService(bf))
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	upsertBody, _ := json.Marshal(map[string]any{
		"wallet_id": "hot",
		"ufvk":      ufvk,
	})
	resp, err := http.Post(srv.URL+"/v1/wallets", "application/json", bytes.NewReader(upsertBody))
	if err != nil {
		t.Fatalf("POST /v1/wallets: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets status=%d", resp.StatusCode)
	}

	// Without backfill, the historic deposit is not detected.
	evs := mustListEvents(t, ctx, srv.URL, "hot")
	if countKind(evs, "DepositEvent") != 0 {
		t.Fatalf("expected no DepositEvent before backfill")
	}

	// Backfill (single batch).
	reqBody, _ := json.Marshal(map[string]any{
		"from_height": 0,
		"batch_size":  10_000,
	})
	bfResp, err := http.Post(srv.URL+"/v1/wallets/hot/backfill", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /v1/wallets/hot/backfill: %v", err)
	}
	_ = bfResp.Body.Close()
	if bfResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets/hot/backfill status=%d", bfResp.StatusCode)
	}

	evs = mustListEvents(t, ctx, srv.URL, "hot")
	if countKind(evs, "DepositEvent") != 1 {
		t.Fatalf("expected DepositEvent after backfill")
	}
	if countKind(evs, "DepositConfirmed") != 1 {
		t.Fatalf("expected DepositConfirmed after backfill")
	}

	// Backfill again should be idempotent (no duplicated DepositEvent).
	bfResp2, err := http.Post(srv.URL+"/v1/wallets/hot/backfill", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /v1/wallets/hot/backfill(2): %v", err)
	}
	_ = bfResp2.Body.Close()
	if bfResp2.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets/hot/backfill(2) status=%d", bfResp2.StatusCode)
	}

	evs2 := mustListEvents(t, ctx, srv.URL, "hot")
	if countKind(evs2, "DepositEvent") != 1 {
		t.Fatalf("expected DepositEvent to remain single after second backfill")
	}
}

func waitForScannerTip(t *testing.T, ctx context.Context, st store.Store, rpc *sdkjunocashd.Client) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	for time.Now().Before(deadline) {
		chainHeight, err := rpc.GetBlockCount(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		tip, ok, err := st.Tip(ctx)
		if err == nil && ok && tip.Height >= chainHeight {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for scanner to catch up")
}

type walletEvent struct {
	Kind string `json:"kind"`
}

func mustListEvents(t *testing.T, ctx context.Context, baseURL string, walletID string) []walletEvent {
	t.Helper()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/wallets/"+walletID+"/events?cursor=0&limit=1000", nil)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET /v1/wallets/%s/events: %v", walletID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/wallets/%s/events status=%d", walletID, resp.StatusCode)
	}
	var out struct {
		Events []walletEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	return out.Events
}

func countKind(evs []walletEvent, kind string) int {
	n := 0
	for _, e := range evs {
		if e.Kind == kind {
			n++
		}
	}
	return n
}
