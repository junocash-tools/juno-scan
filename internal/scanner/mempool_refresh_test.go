package scanner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

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
