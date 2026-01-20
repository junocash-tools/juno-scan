package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestBearerAuthToken(t *testing.T) {
	ctx := context.Background()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	apiServer, err := New(st, WithBearerToken("secret"))
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	t.Run("missing", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/health", nil)
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET /v1/health: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("wrong", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/health", nil)
		req.Header.Set("Authorization", "Bearer nope")
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET /v1/health: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("ok", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/health", nil)
		req.Header.Set("Authorization", "Bearer secret")
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET /v1/health: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})
}
