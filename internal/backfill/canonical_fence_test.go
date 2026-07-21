package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestBackfillFinalBarrenFenceRejectsRollbackRace(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/race.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "exchange", "unused-for-barren-range"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 0, Hash: "old-hash"})
	}); err != nil {
		t.Fatal(err)
	}

	mutationErr := make(chan error, 1)
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request.Method == "getblockhash" {
			if err := st.RollbackToHeight(r.Context(), -1); err == nil {
				err = st.WithTx(r.Context(), func(tx store.Tx) error {
					return tx.InsertBlock(r.Context(), store.Block{Height: 0, Hash: "new-hash"})
				})
			}
			mutationErr <- err
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "old-hash", "error": nil, "id": request.ID})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": nil, "error": map[string]any{"code": -32601, "message": "unexpected method"}, "id": request.ID})
	}))
	defer rpcServer.Close()

	service, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 100)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.BackfillWallet(ctx, Request{WalletID: "exchange", FromHeight: 0, ToHeight: 0, BatchSize: 1})
	if !errors.Is(err, store.ErrCanonicalBlockChanged) {
		t.Fatalf("rollback race err=%v", err)
	}
	if err := <-mutationErr; err != nil {
		t.Fatalf("test mutation: %v", err)
	}
	if hash, ok, err := st.HashAtHeight(ctx, 0); err != nil || !ok || hash != "new-hash" {
		t.Fatalf("canonical replacement missing: hash=%q ok=%v err=%v", hash, ok, err)
	}
}

func TestBackfillRejectsRPCStoredHashMismatch(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/mismatch.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "exchange", "unused-for-barren-range"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 0, Hash: "stored-hash"})
	}); err != nil {
		t.Fatal(err)
	}
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID uint64 `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "rpc-hash", "error": nil, "id": request.ID})
	}))
	defer rpcServer.Close()
	service, _ := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 100)
	_, err = service.BackfillWallet(ctx, Request{WalletID: "exchange", FromHeight: 0, ToHeight: 0, BatchSize: 1})
	if !errors.Is(err, store.ErrCanonicalBlockChanged) {
		t.Fatalf("hash mismatch err=%v", err)
	}
}

func TestBackfillSkipsEmptyHeightsAndCoalescesFinalFence(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/empty-span.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "exchange", "unused-for-barren-range"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		for height := int64(0); height < 10; height++ {
			if err := tx.InsertBlock(ctx, store.Block{Height: height, Hash: fmt.Sprintf("hash-%d", height)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var rpcCalls int
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalls++
		var request struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request.Method != "getblockhash" {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": nil, "error": map[string]any{"code": -32601, "message": "unexpected method"}, "id": request.ID})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "hash-9", "error": nil, "id": request.ID})
	}))
	defer rpcServer.Close()
	service, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 100)
	if err != nil {
		t.Fatal(err)
	}
	res, err := service.BackfillWallet(ctx, Request{WalletID: "exchange", FromHeight: 0, ToHeight: 9, BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if rpcCalls != 1 || res.RPCCalls != 1 || res.VisitedActionHeights != 0 || res.SkippedHeights != 10 || res.NextHeight != 10 {
		t.Fatalf("rpc_calls=%d result=%+v", rpcCalls, res)
	}
}
