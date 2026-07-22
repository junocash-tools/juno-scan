package scanner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestMempoolRefreshStatusStartsUnreadyAndBindsSuccessfulRefresh(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "refresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "hash-10"})
	}); err != nil {
		t.Fatal(err)
	}
	eventEpoch, err := st.EventEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if request.Method != "getrawmempool" {
			t.Errorf("method=%q want getrawmempool", request.Method)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": request.ID, "result": []string{}, "error": nil})
	}))
	defer rpcServer.Close()

	sc, err := New(st, sdkjunocashd.New(rpcServer.URL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if status := sc.Status().MempoolRefresh; status.Ready {
		t.Fatalf("new scanner refresh=%+v", status)
	}
	if err := sc.updatePendingSpends(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if status := sc.Status().MempoolRefresh; !status.Ready || status.EventEpoch != eventEpoch || status.Height != 10 || status.Hash != "hash-10" {
		t.Fatalf("refresh=%+v", status)
	}
}

func TestMempoolRefreshFailsClosedOnGetRawTransactionError(t *testing.T) {
	txid := strings.Repeat("a", 64)
	rpcServer := newScriptedMempoolRPC(t, []mempoolRPCStep{
		{method: "getrawmempool", result: []string{txid}},
		{method: "getrawtransaction", errorCode: -1, errorMessage: "decode unavailable"},
	})
	sc, recording, base := newMempoolFailureScanner(t, rpcServer.URL)

	err := sc.updatePendingSpends(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "getrawtransaction("+txid+")") {
		t.Fatalf("err=%v", err)
	}
	assertFailedRefreshDidNotCommit(t, sc, recording, base)
}

func TestMempoolRefreshIsUnreadyWhileTransactionDecodeIsInFlight(t *testing.T) {
	txid := strings.Repeat("a", 64)
	started := make(chan struct{})
	release := make(chan struct{})
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		response := map[string]any{"id": request.ID, "error": nil}
		switch request.Method {
		case "getrawmempool":
			response["result"] = []string{txid}
		case "getrawtransaction":
			close(started)
			select {
			case <-release:
				response["result"] = nil
				response["error"] = map[string]any{"code": -1, "message": "decode unavailable"}
			case <-r.Context().Done():
				return
			}
		default:
			t.Errorf("unexpected RPC method %q", request.Method)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer rpcServer.Close()
	sc, recording, base := newMempoolFailureScanner(t, rpcServer.URL)
	errCh := make(chan error, 1)
	go func() { errCh <- sc.updatePendingSpends(context.Background(), 10) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("transaction decode did not start")
	}
	if status := sc.Status().MempoolRefresh; status.Ready {
		t.Fatalf("in-flight refresh remained ready: %+v", status)
	}
	if recording.updateCalls != 0 {
		t.Fatalf("UpdatePendingSpends calls=%d while decode is in flight", recording.updateCalls)
	}
	assertSeededPendingPresent(t, base)
	close(release)
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected decode error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not return")
	}
	assertFailedRefreshDidNotCommit(t, sc, recording, base)
}

func TestMempoolRefreshRevalidatesGetRawTransactionMinusFive(t *testing.T) {
	txid := strings.Repeat("b", 64)
	tests := []struct {
		name         string
		revalidation mempoolRPCStep
		wantError    string
		wantReady    bool
		wantUpdates  int
	}{
		{
			name:         "still present",
			revalidation: mempoolRPCStep{method: "getrawmempool", result: []string{txid}},
			wantError:    "remains in mempool",
		},
		{
			name:         "revalidation fails",
			revalidation: mempoolRPCStep{method: "getrawmempool", errorCode: -1, errorMessage: "mempool unavailable"},
			wantError:    "revalidate mempool",
		},
		{
			name:         "vanished",
			revalidation: mempoolRPCStep{method: "getrawmempool", result: []string{}},
			wantReady:    true,
			wantUpdates:  1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpcServer := newScriptedMempoolRPC(t, []mempoolRPCStep{
				{method: "getrawmempool", result: []string{txid}},
				{method: "getrawtransaction", errorCode: -5, errorMessage: "No such mempool transaction"},
				tc.revalidation,
			})
			sc, recording, base := newMempoolFailureScanner(t, rpcServer.URL)

			err := sc.updatePendingSpends(context.Background(), 10)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("err=%v want substring %q", err, tc.wantError)
				}
				assertFailedRefreshDidNotCommit(t, sc, recording, base)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if recording.updateCalls != tc.wantUpdates || sc.Status().MempoolRefresh.Ready != tc.wantReady {
				t.Fatalf("updates=%d refresh=%+v", recording.updateCalls, sc.Status().MempoolRefresh)
			}
			assertSeededPendingCleared(t, base)
		})
	}
}

func TestMempoolRefreshRejectsMalformedIdentitiesAndNullifierCollisions(t *testing.T) {
	txidA := strings.Repeat("a", 64)
	txidB := strings.Repeat("b", 64)
	nullifier := strings.Repeat("c", 64)
	tests := []struct {
		name      string
		steps     []mempoolRPCStep
		wantError string
	}{
		{
			name:      "non-canonical mempool txid",
			steps:     []mempoolRPCStep{{method: "getrawmempool", result: []string{strings.Repeat("A", 64)}}},
			wantError: "non-canonical transaction id",
		},
		{
			name: "non-canonical Orchard nullifier",
			steps: []mempoolRPCStep{
				{method: "getrawmempool", result: []string{txidA}},
				{method: "getrawtransaction", result: rawMempoolTx("not-a-nullifier")},
			},
			wantError: "non-canonical Orchard nullifier",
		},
		{
			name: "cross-transaction nullifier collision",
			steps: []mempoolRPCStep{
				{method: "getrawmempool", result: []string{txidA, txidB}},
				{method: "getrawtransaction", result: rawMempoolTx(nullifier)},
				{method: "getrawtransaction", result: rawMempoolTx(nullifier)},
			},
			wantError: "claim Orchard nullifier",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpcServer := newScriptedMempoolRPC(t, tc.steps)
			sc, recording, base := newMempoolFailureScanner(t, rpcServer.URL)
			err := sc.updatePendingSpends(context.Background(), 10)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("err=%v want substring %q", err, tc.wantError)
			}
			assertFailedRefreshDidNotCommit(t, sc, recording, base)
		})
	}
}

type pendingUpdateRecordingStore struct {
	store.Store
	updateCalls int
}

func (s *pendingUpdateRecordingStore) UpdatePendingSpends(ctx context.Context, pending map[string]store.PendingSpend, chainHeight int64, seenAt time.Time) error {
	s.updateCalls++
	return s.Store.UpdatePendingSpends(ctx, pending, chainHeight, seenAt)
}

func newMempoolFailureScanner(t *testing.T, rpcURL string) (*Scanner, *pendingUpdateRecordingStore, *rocksdb.Store) {
	t.Helper()
	ctx := context.Background()
	base, err := rocksdb.Open(filepath.Join(t.TempDir(), "failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	if err := base.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := base.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	nullifier := strings.Repeat("e", 64)
	if err := base.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "hash-10"}); err != nil {
			return err
		}
		_, err := tx.InsertNote(ctx, store.Note{
			WalletID:      "hot",
			TxID:          strings.Repeat("d", 64),
			Height:        1,
			ValueZat:      100,
			NoteNullifier: nullifier,
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(5)
	if err := base.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		nullifier: {TxID: strings.Repeat("f", 64), ExpiryHeight: &expiry},
	}, 4, time.Unix(1, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	recording := &pendingUpdateRecordingStore{Store: base}
	sc, err := New(recording, sdkjunocashd.New(rpcURL, "", ""), "jregtest", 0, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := base.EventEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tip, found, err := base.Tip(ctx)
	if err != nil || !found {
		t.Fatalf("tip found=%v err=%v", found, err)
	}
	sc.recordMempoolRefresh(epoch, tip)
	return sc, recording, base
}

func assertFailedRefreshDidNotCommit(t *testing.T, sc *Scanner, recording *pendingUpdateRecordingStore, base *rocksdb.Store) {
	t.Helper()
	if recording.updateCalls != 0 {
		t.Fatalf("UpdatePendingSpends calls=%d want 0", recording.updateCalls)
	}
	if status := sc.Status().MempoolRefresh; status.Ready {
		t.Fatalf("failed refresh remained ready: %+v", status)
	}
	if len(sc.mempoolOrchardNullifiersByTxID) != 0 || len(sc.mempoolTxExpiryHeightByTxID) != 0 {
		t.Fatalf("partial decode entered cache: nullifiers=%v expiry=%v", sc.mempoolOrchardNullifiersByTxID, sc.mempoolTxExpiryHeightByTxID)
	}
	assertSeededPendingPresent(t, base)
}

func assertSeededPendingPresent(t *testing.T, base *rocksdb.Store) {
	t.Helper()
	notes, err := base.ListWalletNotes(context.Background(), "hot", true, 10)
	if err != nil || len(notes) != 1 {
		t.Fatalf("notes=%+v err=%v", notes, err)
	}
	if notes[0].PendingSpentTxID == nil || *notes[0].PendingSpentTxID != strings.Repeat("f", 64) || notes[0].PendingSpentExpiryHeight == nil || *notes[0].PendingSpentExpiryHeight != 5 {
		t.Fatalf("seeded pending mutated: %+v", notes[0])
	}
}

func assertSeededPendingCleared(t *testing.T, base *rocksdb.Store) {
	t.Helper()
	notes, err := base.ListWalletNotes(context.Background(), "hot", true, 10)
	if err != nil || len(notes) != 1 {
		t.Fatalf("notes=%+v err=%v", notes, err)
	}
	if notes[0].PendingSpentTxID != nil || notes[0].PendingSpentAt != nil || notes[0].PendingSpentExpiryHeight != nil {
		t.Fatalf("expired pending was not cleared after successful refresh: %+v", notes[0])
	}
}

type mempoolRPCStep struct {
	method       string
	result       any
	errorCode    int
	errorMessage string
}

func newScriptedMempoolRPC(t *testing.T, steps []mempoolRPCStep) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	next := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if next >= len(steps) {
			t.Errorf("unexpected RPC method %q", request.Method)
			return
		}
		step := steps[next]
		next++
		if request.Method != step.method {
			t.Errorf("RPC step %d method=%q want=%q", next, request.Method, step.method)
			return
		}
		response := map[string]any{"id": request.ID, "result": step.result, "error": nil}
		if step.errorCode != 0 {
			response["result"] = nil
			response["error"] = map[string]any{"code": step.errorCode, "message": step.errorMessage}
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(func() {
		server.Close()
		mu.Lock()
		defer mu.Unlock()
		if next != len(steps) {
			t.Errorf("RPC steps consumed=%d want=%d", next, len(steps))
		}
	})
	return server
}

func rawMempoolTx(nullifiers ...string) map[string]any {
	actions := make([]map[string]any, 0, len(nullifiers))
	for _, nullifier := range nullifiers {
		actions = append(actions, map[string]any{"nullifier": nullifier})
	}
	return map[string]any{
		"hex":          "",
		"expiryheight": 20,
		"orchard":      map[string]any{"actions": actions},
	}
}
