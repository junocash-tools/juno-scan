//go:build integration

package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestHandleUpsertWallet_InvalidUFVK(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	srv, err := New(st)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}

	body := []byte(`{"wallet_id":"hot","ufvk":"jview1test"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/wallets", bytes.NewReader(body))

	srv.handleUpsertWallet(rr, req)

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(b))
	}
}
