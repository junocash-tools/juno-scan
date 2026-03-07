package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestListWalletNotes_PaginationAndMinValue(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if _, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "0000000000000000000000000000000000000000000000000000000000000001",
			ActionIndex:      0,
			Height:           10,
			RecipientAddress: "jtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqp4f3t7",
			ValueZat:         5,
			NoteNullifier:    "1000000000000000000000000000000000000000000000000000000000000001",
		}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "0000000000000000000000000000000000000000000000000000000000000002",
			ActionIndex:      0,
			Height:           11,
			RecipientAddress: "jtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqp4f3t7",
			ValueZat:         20,
			NoteNullifier:    "2000000000000000000000000000000000000000000000000000000000000002",
		}); err != nil {
			return err
		}
		_, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "0000000000000000000000000000000000000000000000000000000000000003",
			ActionIndex:      0,
			Height:           12,
			RecipientAddress: "jtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqp4f3t7",
			ValueZat:         30,
			NoteNullifier:    "3000000000000000000000000000000000000000000000000000000000000003",
		})
		return err
	}); err != nil {
		t.Fatalf("Insert notes: %v", err)
	}

	apiServer, err := New(st)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	type walletNote struct {
		TxID     string `json:"txid"`
		ValueZat int64  `json:"value_zat"`
	}
	type notesResponse struct {
		Notes      []walletNote `json:"notes"`
		NextCursor string       `json:"next_cursor"`
	}

	req1, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/notes?limit=1&min_value_zat=10", nil)
	httpResp1, err := (&http.Client{}).Do(req1)
	if err != nil {
		t.Fatalf("GET notes page1: %v", err)
	}
	defer httpResp1.Body.Close()
	if httpResp1.StatusCode != http.StatusOK {
		t.Fatalf("GET notes page1 status=%d", httpResp1.StatusCode)
	}
	var out1 notesResponse
	if err := json.NewDecoder(httpResp1.Body).Decode(&out1); err != nil {
		t.Fatalf("decode page1: %v", err)
	}
	if len(out1.Notes) != 1 {
		t.Fatalf("page1 notes=%d", len(out1.Notes))
	}
	if out1.Notes[0].TxID != "0000000000000000000000000000000000000000000000000000000000000002" {
		t.Fatalf("unexpected first txid=%q", out1.Notes[0].TxID)
	}
	if out1.NextCursor == "" {
		t.Fatalf("expected next_cursor")
	}

	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/notes?limit=1&min_value_zat=10&cursor="+out1.NextCursor, nil)
	httpResp2, err := (&http.Client{}).Do(req2)
	if err != nil {
		t.Fatalf("GET notes page2: %v", err)
	}
	defer httpResp2.Body.Close()
	if httpResp2.StatusCode != http.StatusOK {
		t.Fatalf("GET notes page2 status=%d", httpResp2.StatusCode)
	}
	var out2 notesResponse
	if err := json.NewDecoder(httpResp2.Body).Decode(&out2); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if len(out2.Notes) != 1 {
		t.Fatalf("page2 notes=%d", len(out2.Notes))
	}
	if out2.Notes[0].TxID != "0000000000000000000000000000000000000000000000000000000000000003" {
		t.Fatalf("unexpected second txid=%q", out2.Notes[0].TxID)
	}
	if out2.NextCursor != "" {
		t.Fatalf("unexpected next_cursor=%q", out2.NextCursor)
	}
}

func TestListWalletNotes_InvalidCursor(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	apiServer, err := New(st)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/notes?cursor=bad-cursor", nil)
	httpResp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("GET notes: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", httpResp.StatusCode)
	}
}

func TestListWalletNotes_IncludesMemoHexField(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	memoHex := "00"
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if _, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "0000000000000000000000000000000000000000000000000000000000000001",
			ActionIndex:      0,
			Height:           10,
			RecipientAddress: "jtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqp4f3t7",
			ValueZat:         5,
			NoteNullifier:    "1000000000000000000000000000000000000000000000000000000000000001",
		}); err != nil {
			return err
		}

		_, err := tx.InsertNote(ctx, store.Note{
			WalletID:         "hot",
			TxID:             "0000000000000000000000000000000000000000000000000000000000000002",
			ActionIndex:      0,
			Height:           11,
			RecipientAddress: "jtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqp4f3t7",
			ValueZat:         10,
			MemoHex:          &memoHex,
			NoteNullifier:    "2000000000000000000000000000000000000000000000000000000000000002",
		})
		return err
	}); err != nil {
		t.Fatalf("Insert notes: %v", err)
	}

	apiServer, err := New(st)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/notes?spent=true&limit=10", nil)
	httpResp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("GET notes: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		t.Fatalf("GET notes status=%d", httpResp.StatusCode)
	}

	var out struct {
		Notes []map[string]json.RawMessage `json:"notes"`
	}
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
		t.Fatalf("decode notes: %v", err)
	}
	if len(out.Notes) != 2 {
		t.Fatalf("notes=%d", len(out.Notes))
	}

	firstMemo, ok := out.Notes[0]["memo_hex"]
	if !ok {
		t.Fatalf("first note missing memo_hex field")
	}
	if !bytes.Equal(bytes.TrimSpace(firstMemo), []byte("null")) {
		t.Fatalf("expected null memo_hex for note without memo, got %s", string(firstMemo))
	}

	secondMemo, ok := out.Notes[1]["memo_hex"]
	if !ok {
		t.Fatalf("second note missing memo_hex field")
	}
	var gotMemo string
	if err := json.Unmarshal(secondMemo, &gotMemo); err != nil {
		t.Fatalf("unmarshal memo_hex: %v", err)
	}
	if gotMemo != memoHex {
		t.Fatalf("memo_hex=%q", gotMemo)
	}
}
