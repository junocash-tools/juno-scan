package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestWalletNoteSummaryEndpoint(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/scan.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	position := int64(1)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "h10"}); err != nil {
			return err
		}
		for _, note := range []store.Note{
			{WalletID: "hot", TxID: txidForTest('a'), Height: 1, Position: &position, ValueZat: 200, NoteNullifier: txidForTest('1')},
			{WalletID: "hot", TxID: txidForTest('b'), Height: 9, ValueZat: 300, NoteNullifier: txidForTest('2')},
		} {
			if _, err := tx.InsertNote(ctx, note); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		txidForTest('2'): {TxID: txidForTest('c')},
	}, 10, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	server, err := New(st, WithBearerToken("scanner-secret"))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/summary", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/summary?min_confirmations=5&min_note_zat=100&max_notes=2", nil)
	req.Header.Set("Authorization", "Bearer scanner-secret")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got apiWalletNoteSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.WalletID != "hot" || got.MinConfirmations != 5 || got.MinNoteZat != 100 || got.AsOfScannerHeight != 10 || got.AsOfScannerHash != "h10" {
		t.Fatalf("identity=%+v", got)
	}
	if got.TotalUnspent.NoteCount != 2 || got.TotalUnspent.ValueZat != 500 || got.Spendable.NoteCount != 1 || got.Spendable.ValueZat != 200 || got.PendingSpend.NoteCount != 1 || got.PendingSpend.ValueZat != 300 {
		t.Fatalf("summary=%+v", got)
	}
	assertSummaryJSONPresence(t, rr.Body.Bytes())

	for _, path := range []string{
		"/v1/wallets/hot/notes/summary?min_confirmations=-1",
		"/v1/wallets/hot/notes/summary?min_note_zat=-1",
		"/v1/wallets/hot/notes/summary?max_notes=0",
		"/v1/wallets/hot/notes/summary?max_notes=1000001",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer scanner-secret")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/summary?max_notes=1", nil)
	req.Header.Set("Authorization", "Bearer scanner-secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnprocessableEntity || rr.Body.String() != "wallet note inventory exceeds max_notes\n" {
		t.Fatalf("limit status=%d body=%q", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/missing/notes/summary", nil)
	req.Header.Set("Authorization", "Bearer scanner-secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/not-summary", nil)
	req.Header.Set("Authorization", "Bearer scanner-secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown notes subroute status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWalletNoteSummaryEndpointScannerNotReady(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/scan.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	server, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/summary", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func assertSummaryJSONPresence(t *testing.T, body []byte) {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"wallet_id", "min_confirmations", "min_note_zat", "as_of_scanner_height", "as_of_scanner_hash", "total_unspent", "spendable", "immature", "pending_spend", "below_min_note", "witness_unavailable"} {
		if _, ok := root[key]; !ok {
			t.Fatalf("response omitted %q: %s", key, body)
		}
	}
	for _, bucketName := range []string{"total_unspent", "spendable", "immature", "pending_spend", "below_min_note", "witness_unavailable"} {
		var bucket map[string]json.RawMessage
		if err := json.Unmarshal(root[bucketName], &bucket); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"note_count", "value_zat"} {
			if _, ok := bucket[key]; !ok {
				t.Fatalf("%s omitted %s: %s", bucketName, key, body)
			}
		}
	}
	for bucketName, keys := range map[string][]string{
		"spendable":     {"smallest_note_zat", "largest_note_zat"},
		"pending_spend": {"known_expiry_count", "next_expiry_height", "last_expiry_height"},
	} {
		var bucket map[string]json.RawMessage
		if err := json.Unmarshal(root[bucketName], &bucket); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if _, ok := bucket[key]; !ok {
				t.Fatalf("%s omitted %s: %s", bucketName, key, body)
			}
		}
	}
}
