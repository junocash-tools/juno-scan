package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/backfill"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestBackfillAPIOnlyAdvancesPersistedContiguousProgress(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/progress.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "unused-for-barren-range", 0); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 0, Hash: "hash-0"})
	}); err != nil {
		t.Fatal(err)
	}
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID uint64 `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "hash-0", "error": nil, "id": request.ID})
	}))
	defer rpcServer.Close()
	bf, err := backfill.New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 100)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(st, WithBackfillService(bf))
	if err != nil {
		t.Fatal(err)
	}
	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/wallets/exchange/backfill", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		return rr
	}
	if rr := post(`{"from_height":1,"to_height":0}`); rr.Code != http.StatusConflict {
		t.Fatalf("non-contiguous from_height status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := post(`{"from_height":0,"to_height":0}`); rr.Code != http.StatusOK {
		t.Fatalf("contiguous backfill status=%d body=%s", rr.Code, rr.Body.String())
	}
	progress, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	if progress.NextHeight != 1 || progress.State != "complete" || progress.TargetHeight != 0 {
		t.Fatalf("progress=%+v", progress)
	}
	if rr := post(`{}`); rr.Code != http.StatusOK {
		t.Fatalf("completed default retry status=%d body=%s", rr.Code, rr.Body.String())
	}
	progressAfterRetry, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	if progressAfterRetry.NextHeight != progress.NextHeight || progressAfterRetry.State != "complete" || progressAfterRetry.LastError != "" {
		t.Fatalf("completed default retry mutated progress: before=%+v after=%+v", progress, progressAfterRetry)
	}
	healthReq := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	healthRR := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthRR, healthReq)
	var health struct {
		Ready bool `json:"ready"`
	}
	if healthRR.Code != http.StatusOK || json.Unmarshal(healthRR.Body.Bytes(), &health) != nil || !health.Ready {
		t.Fatalf("health after completed retry status=%d body=%s", healthRR.Code, healthRR.Body.String())
	}
	if rr := post(`{"to_height":0}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid completed range status=%d body=%s", rr.Code, rr.Body.String())
	}
	progressAfterInvalid, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	if progressAfterInvalid.State != "complete" || progressAfterInvalid.LastError != "" {
		t.Fatalf("invalid range persisted an error: %+v", progressAfterInvalid)
	}
	if rr := post(`{"from_height":0,"to_height":0}`); rr.Code != http.StatusConflict {
		t.Fatalf("stale replay status=%d body=%s", rr.Code, rr.Body.String())
	}
}
