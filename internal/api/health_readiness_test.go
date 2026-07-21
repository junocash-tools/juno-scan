package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestHealthRequiresTipAndBackfillBeyondTip(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/health.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wallet := range []struct{ id, ufvk string }{{"exchange-a", "ufvk-a"}, {"exchange-b", "ufvk-b"}} {
		if err := st.UpsertWalletBirthday(ctx, wallet.id, wallet.ufvk, 0); err != nil {
			t.Fatal(err)
		}
	}
	server, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	checkReady := func(want bool) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		rr := httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		var response struct {
			Ready         bool  `json:"ready"`
			Confirmations int64 `json:"confirmations"`
		}
		if rr.Code != http.StatusOK || json.Unmarshal(rr.Body.Bytes(), &response) != nil {
			t.Fatalf("health status=%d body=%s", rr.Code, rr.Body.String())
		}
		if response.Ready != want {
			t.Fatalf("ready=%v want %v body=%s", response.Ready, want, rr.Body.String())
		}
		if response.Confirmations != 100 {
			t.Fatalf("confirmations=%d want 100 body=%s", response.Confirmations, rr.Body.String())
		}
	}

	checkReady(false)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "hash-10"})
	}); err != nil {
		t.Fatal(err)
	}
	checkReady(false)
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		progress.NextHeight, progress.TargetHeight, progress.State = 10, 10, "complete"
		if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
			t.Fatal(err)
		}
	}
	checkReady(false)
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		progress.NextHeight = 11
		if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
			t.Fatal(err)
		}
	}
	checkReady(true)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 11, Hash: "hash-11"}); err != nil {
			return err
		}
		return tx.AdvanceCompleteWalletBackfillProgress(ctx, 11)
	}); err != nil {
		t.Fatal(err)
	}
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		if progress.NextHeight != 12 || progress.State != "complete" {
			t.Fatalf("%s progress did not follow live tip: %+v", walletID, progress)
		}
	}
	checkReady(true)

	// Simulate the scanner committing height 12 while an API backfill request
	// still holds a running progress snapshot. The next live block must catch the
	// subsequently completed wallet up instead of requiring another backfill.
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		progress.State = "running"
		if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 12, Hash: "hash-12"}); err != nil {
			return err
		}
		return tx.AdvanceCompleteWalletBackfillProgress(ctx, 12)
	}); err != nil {
		t.Fatal(err)
	}
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		progress.State, progress.TargetHeight = "complete", 11
		if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 13, Hash: "hash-13"}); err != nil {
			return err
		}
		return tx.AdvanceCompleteWalletBackfillProgress(ctx, 13)
	}); err != nil {
		t.Fatal(err)
	}
	for _, walletID := range []string{"exchange-a", "exchange-b"} {
		progress, _, _ := st.WalletBackfillStatus(ctx, walletID)
		if progress.NextHeight != 14 || progress.State != "complete" || progress.TargetHeight != 13 {
			t.Fatalf("%s missed-live-block progress did not catch up: %+v", walletID, progress)
		}
	}
	checkReady(true)
}

func TestHealthDegradesWhenScannerTipIsAheadOfNode(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/scanner-ahead.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "stored-tip"})
	}); err != nil {
		t.Fatal(err)
	}

	server, err := New(st, WithRuntimeStatus("regtest", "jregtest", 100, 2, func() (int64, bool) {
		return 9, true
	}))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	var response struct {
		Status        string `json:"status"`
		Ready         bool   `json:"ready"`
		NodeHeight    int64  `json:"node_height"`
		ScannedHeight int64  `json:"scanned_height"`
		ScannerLag    int64  `json:"scanner_lag"`
	}
	if rr.Code != http.StatusOK || json.Unmarshal(rr.Body.Bytes(), &response) != nil {
		t.Fatalf("health status=%d body=%s", rr.Code, rr.Body.String())
	}
	if response.Ready || response.Status != "degraded" || response.NodeHeight != 9 || response.ScannedHeight != 10 || response.ScannerLag != 0 {
		t.Fatalf("scanner-ahead health=%+v body=%s", response, rr.Body.String())
	}
}
