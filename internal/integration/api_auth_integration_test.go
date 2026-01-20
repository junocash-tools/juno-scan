//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/api"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestIntegration_API_BearerAuth(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	apiServer, err := api.New(st, api.WithBearerToken("secret"))
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/health", nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("GET /v1/health: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/health", nil)
	req2.Header.Set("Authorization", "Bearer secret")
	resp2, err := (&http.Client{}).Do(req2)
	if err != nil {
		t.Fatalf("GET /v1/health (auth): %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
}
