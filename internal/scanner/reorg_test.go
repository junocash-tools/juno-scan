package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestReconcileCanonicalTipRollsBackWhenScannerIsAhead(t *testing.T) {
	ctx := context.Background()
	st := openReorgStore(t, "hash-0", "hash-1", "stale-hash-2")
	rpcServer, calls := newBlockHashRPC(t, map[int64]string{0: "hash-0", 1: "hash-1"}, nil)
	defer rpcServer.Close()
	sc, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}

	tip := mustStoreTip(t, st)
	rolledBack, err := sc.reconcileCanonicalTip(ctx, tip, 1)
	if err != nil || !rolledBack {
		t.Fatalf("rolled_back=%v err=%v", rolledBack, err)
	}
	if got := mustStoreTip(t, st); got.Height != 1 || got.Hash != "hash-1" {
		t.Fatalf("tip after rollback=%+v", got)
	}
	if got := fmt.Sprint(*calls); got != "[1]" {
		t.Fatalf("getblockhash heights=%s want [1]", got)
	}
}

func TestReconcileCanonicalTipRollsBackSameHeightReplacement(t *testing.T) {
	ctx := context.Background()
	st := openReorgStore(t, "hash-0", "stale-hash-1")
	rpcServer, calls := newBlockHashRPC(t, map[int64]string{0: "hash-0", 1: "replacement-hash-1"}, nil)
	defer rpcServer.Close()
	sc, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}

	rolledBack, err := sc.reconcileCanonicalTip(ctx, mustStoreTip(t, st), 1)
	if err != nil || !rolledBack {
		t.Fatalf("rolled_back=%v err=%v", rolledBack, err)
	}
	if got := mustStoreTip(t, st); got.Height != 0 || got.Hash != "hash-0" {
		t.Fatalf("tip after replacement rollback=%+v", got)
	}
	if got := fmt.Sprint(*calls); got != "[1 0]" {
		t.Fatalf("getblockhash heights=%s want [1 0]", got)
	}
}

func TestReconcileCanonicalTipLeavesStoreIntactOnAncestorRPCError(t *testing.T) {
	ctx := context.Background()
	st := openReorgStore(t, "hash-0", "hash-1", "stale-hash-2")
	rpcServer, _ := newBlockHashRPC(t, map[int64]string{0: "hash-0"}, map[int64]string{1: "temporary rpc failure"})
	defer rpcServer.Close()
	sc, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}

	rolledBack, err := sc.reconcileCanonicalTip(ctx, mustStoreTip(t, st), 1)
	if err == nil || rolledBack || !strings.Contains(err.Error(), "temporary rpc failure") {
		t.Fatalf("rolled_back=%v err=%v", rolledBack, err)
	}
	if got := mustStoreTip(t, st); got.Height != 2 || got.Hash != "stale-hash-2" {
		t.Fatalf("rpc error mutated tip=%+v", got)
	}
}

func TestScanOnceDoesNotCommitBlockWithUnexpectedParent(t *testing.T) {
	ctx := context.Background()
	st := openReorgStore(t, "hash-0")
	getBlockCountCalls := 0
	getBlockCalls := 0
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode rpc request: %v", err)
			return
		}
		response := map[string]any{"id": request.ID, "error": nil}
		switch request.Method {
		case "getblockcount":
			getBlockCountCalls++
			if getBlockCountCalls == 1 {
				response["result"] = 1
			} else {
				response["result"] = nil
				response["error"] = map[string]any{"code": -1, "message": "stop after parent mismatch"}
			}
		case "getblockhash":
			var height int64
			if len(request.Params) != 1 || json.Unmarshal(request.Params[0], &height) != nil {
				t.Errorf("invalid getblockhash params: %s", request.Params)
				return
			}
			response["result"] = fmt.Sprintf("hash-%d", height)
		case "getblock":
			getBlockCalls++
			response["result"] = map[string]any{
				"hash":              "hash-1",
				"height":            1,
				"time":              1,
				"previousblockhash": "replacement-parent",
				"tx":                []any{},
			}
		default:
			t.Errorf("unexpected rpc method: %s", request.Method)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encode rpc response: %v", err)
		}
	}))
	defer rpcServer.Close()
	sc, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}

	err = sc.scanOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "stop after parent mismatch") {
		t.Fatalf("scanOnce err=%v", err)
	}
	if getBlockCalls != 1 {
		t.Fatalf("getblock calls=%d want 1", getBlockCalls)
	}
	if got := mustStoreTip(t, st); got.Height != 0 || got.Hash != "hash-0" {
		t.Fatalf("unexpected-parent block mutated tip=%+v", got)
	}
}

func openReorgStore(t *testing.T, hashes ...string) store.Store {
	t.Helper()
	ctx := context.Background()
	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "reorg.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		for height, hash := range hashes {
			previous := ""
			if height > 0 {
				previous = hashes[height-1]
			}
			if err := tx.InsertBlock(ctx, store.Block{Height: int64(height), Hash: hash, PrevHash: previous}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return st
}

func mustStoreTip(t *testing.T, st store.Store) store.BlockTip {
	t.Helper()
	tip, ok, err := st.Tip(context.Background())
	if err != nil || !ok {
		t.Fatalf("tip ok=%v err=%v", ok, err)
	}
	return tip
}

func newBlockHashRPC(t *testing.T, hashes map[int64]string, failures map[int64]string) (*httptest.Server, *[]int64) {
	t.Helper()
	calls := make([]int64, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode rpc request: %v", err)
			return
		}
		if request.Method != "getblockhash" || len(request.Params) != 1 {
			t.Errorf("unexpected rpc request: method=%q params=%s", request.Method, request.Params)
			return
		}
		var height int64
		if err := json.Unmarshal(request.Params[0], &height); err != nil {
			t.Errorf("decode height: %v", err)
			return
		}
		calls = append(calls, height)
		response := map[string]any{"id": request.ID, "result": hashes[height], "error": nil}
		if message, failed := failures[height]; failed {
			response["result"] = nil
			response["error"] = map[string]any{"code": -1, "message": message}
		} else if _, exists := hashes[height]; !exists {
			response["result"] = nil
			response["error"] = map[string]any{"code": -8, "message": "height out of range"}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encode rpc response: %v", err)
		}
	}))
	return server, &calls
}
