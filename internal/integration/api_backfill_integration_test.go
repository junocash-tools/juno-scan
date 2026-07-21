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
	"strconv"
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
	_, otherUFVK := mustCreateWalletAndUFVK(t, ctx, jd)
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
	ensureMatureCoinbaseUTXO(t, ctx, jd)
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "2"))
	toAddr := mustCreateUnifiedAddress(t, ctx, jd)
	withdrawOpID := mustSendMany(t, ctx, jd, addr, toAddr, "0.01")
	mustWaitOpSuccess(t, ctx, jd, withdrawOpID)
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
		"wallet_id":       "hot",
		"ufvk":            ufvk,
		"birthday_height": 10,
	})
	resp, err := http.Post(srv.URL+"/v1/wallets", "application/json", bytes.NewReader(upsertBody))
	if err != nil {
		t.Fatalf("POST /v1/wallets: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		t.Fatalf("POST /v1/wallets status=%d", resp.StatusCode)
	}
	var registration struct {
		UFVKFingerprint string `json:"ufvk_fingerprint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registration); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if registration.UFVKFingerprint != store.UFVKFingerprint(ufvk) {
		t.Fatalf("registration fingerprint=%q", registration.UFVKFingerprint)
	}
	if status := postWalletStatus(t, srv.URL, map[string]any{"wallet_id": "hot", "ufvk": otherUFVK, "birthday_height": 10}); status != http.StatusConflict {
		t.Fatalf("changed UFVK status=%d", status)
	}
	if status := postWalletStatus(t, srv.URL, map[string]any{"wallet_id": "other", "ufvk": ufvk, "birthday_height": 10}); status != http.StatusConflict {
		t.Fatalf("duplicate UFVK status=%d", status)
	}
	if status := postWalletStatus(t, srv.URL, map[string]any{"wallet_id": "hot", "ufvk": ufvk, "birthday_height": 11}); status != http.StatusConflict {
		t.Fatalf("birthday increase status=%d", status)
	}
	if status := postWalletStatus(t, srv.URL, map[string]any{"wallet_id": "hot", "ufvk": ufvk, "birthday_height": 0}); status != http.StatusOK {
		t.Fatalf("birthday rewind status=%d", status)
	}
	statusResp, err := http.Get(srv.URL + "/v1/wallets/hot/backfill")
	if err != nil {
		t.Fatal(err)
	}
	var backfillStatus store.WalletBackfillProgress
	if err := json.NewDecoder(statusResp.Body).Decode(&backfillStatus); err != nil {
		_ = statusResp.Body.Close()
		t.Fatal(err)
	}
	_ = statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK || backfillStatus.UFVKFingerprint != store.UFVKFingerprint(ufvk) {
		t.Fatalf("backfill status=%d fingerprint=%q", statusResp.StatusCode, backfillStatus.UFVKFingerprint)
	}

	// Without backfill, the historic deposit is not detected.
	evs := mustListEvents(t, ctx, srv.URL, "hot", nil)
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

	evs = mustListEvents(t, ctx, srv.URL, "hot", nil)
	if countKind(evs, "DepositEvent") != 1 {
		t.Fatalf("expected DepositEvent after backfill")
	}
	if countKind(evs, "DepositConfirmed") != 1 {
		t.Fatalf("expected DepositConfirmed after backfill")
	}
	if countKind(evs, "SpendEvent") != 1 {
		t.Fatalf("expected withdrawal SpendEvent after backfill")
	}

	// Backfill again should be idempotent (no duplicated DepositEvent).
	bfResp2, err := http.Post(srv.URL+"/v1/wallets/hot/backfill", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("POST /v1/wallets/hot/backfill(2): %v", err)
	}
	_ = bfResp2.Body.Close()
	if bfResp2.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/wallets/hot/backfill(2) status=%d", bfResp2.StatusCode)
	}

	evs2 := mustListEvents(t, ctx, srv.URL, "hot", nil)
	if countKind(evs2, "DepositEvent") != 1 {
		t.Fatalf("expected DepositEvent to remain single after second backfill")
	}

	// block_height filter (debug/audit)
	depositHeight := int64(-1)
	for _, e := range evs2 {
		if e.Kind == "DepositEvent" {
			depositHeight = e.Height
			break
		}
	}
	if depositHeight < 0 {
		t.Fatalf("expected DepositEvent with a height")
	}

	evsAtHeight := mustListEvents(t, ctx, srv.URL, "hot", &depositHeight)
	if countKind(evsAtHeight, "DepositEvent") != 1 {
		t.Fatalf("expected DepositEvent via block_height filter")
	}
	for _, e := range evsAtHeight {
		if e.Height != depositHeight {
			t.Fatalf("expected all filtered events at height=%d, got %+v", depositHeight, evsAtHeight)
		}
	}
}

func TestIntegration_BackfillCrossWalletTransferOrderIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatal(err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()
	addrA, ufvkA := mustCreateWalletAndUFVK(t, ctx, jd)
	addrB, ufvkB := mustCreateWalletAndUFVK(t, ctx, jd)
	uaHRP := strings.SplitN(addrA, "1", 2)[0]
	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	ensureMatureCoinbaseUTXO(t, ctx, jd)
	shieldOpID := mustShieldCoinbase(t, ctx, jd, mustCoinbaseAddress(t, ctx, jd), addrA)
	mustWaitOpSuccess(t, ctx, jd, shieldOpID)
	mustRun(t, jd.CLICommand(ctx, "generate", "2"))
	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvkA, 2)
	transferOpID := mustSendMany(t, ctx, jd, addrA, addrB, "0.01")
	mustWaitOpSuccess(t, ctx, jd, transferOpID)
	transferTxID := mustTxIDForOpID(t, ctx, jd, transferOpID)
	mustRun(t, jd.CLICommand(ctx, "generate", "2"))
	tip, err := rpc.GetBlockCount(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, order := range [][]string{{"wallet-a", "wallet-b"}, {"wallet-b", "wallet-a"}} {
		order := order
		t.Run(strings.Join(order, "-then-"), func(t *testing.T) {
			st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if err := st.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			sc, err := scanner.New(st, rpc, uaHRP, 50*time.Millisecond, 2, "")
			if err != nil {
				t.Fatal(err)
			}
			runCtx, runCancel := context.WithCancel(ctx)
			errCh := make(chan error, 1)
			go func() { errCh <- sc.Run(runCtx) }()
			waitForScannerTip(t, ctx, st, rpc)
			runCancel()
			select {
			case <-errCh:
			case <-time.After(5 * time.Second):
				t.Fatal("scanner did not stop")
			}
			if err := st.UpsertWallet(ctx, "wallet-a", ufvkA); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertWallet(ctx, "wallet-b", ufvkB); err != nil {
				t.Fatal(err)
			}
			bf, err := backfill.New(st, rpc, uaHRP, 2)
			if err != nil {
				t.Fatal(err)
			}
			for _, walletID := range order {
				if _, err := bf.BackfillWallet(ctx, backfill.Request{WalletID: walletID, FromHeight: 0, ToHeight: tip, BatchSize: tip + 1}); err != nil {
					t.Fatalf("backfill %s: %v", walletID, err)
				}
			}
			eventsB, _, err := st.ListWalletEvents(ctx, "wallet-b", 0, 1000, store.EventFilter{TxID: transferTxID})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range eventsB {
				if strings.HasPrefix(event.Kind, "Deposit") {
					t.Fatalf("order %v exposed cross-wallet deposit: %+v", order, event)
				}
			}
			eventsA, _, err := st.ListWalletEvents(ctx, "wallet-a", 0, 1000, store.EventFilter{TxID: transferTxID})
			if err != nil {
				t.Fatal(err)
			}
			outgoingEvents := 0
			for _, event := range eventsA {
				if event.Kind == "OutgoingOutputEvent" {
					outgoingEvents++
				}
			}
			if outgoingEvents != 1 {
				t.Fatalf("order %v outgoing recovery events=%d want 1: %+v", order, outgoingEvents, eventsA)
			}
		})
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
		chainHash, err := rpc.GetBlockHash(ctx, chainHeight)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		tip, ok, err := st.Tip(ctx)
		if err == nil && ok && tip.Height == chainHeight && tip.Hash == chainHash {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for scanner to catch up")
}

type walletEvent struct {
	Kind   string `json:"kind"`
	Height int64  `json:"height"`
}

func mustListEvents(t *testing.T, ctx context.Context, baseURL string, walletID string, blockHeight *int64) []walletEvent {
	t.Helper()

	url := baseURL + "/v1/wallets/" + walletID + "/events?cursor=0&limit=1000"
	if blockHeight != nil {
		url += "&block_height=" + strconv.FormatInt(*blockHeight, 10)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

func postWalletStatus(t *testing.T, baseURL string, body map[string]any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+"/v1/wallets", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
