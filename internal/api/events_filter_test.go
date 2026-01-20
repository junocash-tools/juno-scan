package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestListWalletEvents_BlockHeightFilter(t *testing.T) {
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
		if err := tx.InsertEvent(ctx, store.Event{Kind: "A", WalletID: "hot", Height: 10, Payload: json.RawMessage(`{"n":1}`)}); err != nil {
			return err
		}
		if err := tx.InsertEvent(ctx, store.Event{Kind: "B", WalletID: "hot", Height: 11, Payload: json.RawMessage(`{"n":2}`)}); err != nil {
			return err
		}
		return tx.InsertEvent(ctx, store.Event{Kind: "C", WalletID: "hot", Height: 10, Payload: json.RawMessage(`{"n":3}`)})
	}); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	apiServer, err := New(st)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}

	srv := httptest.NewServer(apiServer.Handler())
	defer srv.Close()

	type resp struct {
		Events []struct {
			ID     int64  `json:"id"`
			Kind   string `json:"kind"`
			Height int64  `json:"height"`
		} `json:"events"`
		NextCursor int64 `json:"next_cursor"`
	}

	t.Run("unfiltered", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/events?cursor=0&limit=1000", nil)
		httpResp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET events: %v", err)
		}
		defer httpResp.Body.Close()
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("GET events status=%d", httpResp.StatusCode)
		}

		var out resp
		if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(out.Events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(out.Events))
		}
		if out.NextCursor == 0 {
			t.Fatalf("expected next_cursor > 0")
		}
	})

	t.Run("filtered", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/events?cursor=0&limit=1000&block_height=10", nil)
		httpResp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET events: %v", err)
		}
		defer httpResp.Body.Close()
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("GET events status=%d", httpResp.StatusCode)
		}

		var out resp
		if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(out.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(out.Events))
		}
		for _, e := range out.Events {
			if e.Height != 10 {
				t.Fatalf("expected height=10, got %d", e.Height)
			}
		}
	})

	t.Run("invalid", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/wallets/hot/events?cursor=0&limit=1000&block_height=nope", nil)
		httpResp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Fatalf("GET events: %v", err)
		}
		defer httpResp.Body.Close()
		if httpResp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", httpResp.StatusCode)
		}
	})
}
