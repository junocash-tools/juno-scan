package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestWalletNoteStatusesEndpointStatesAndOrder(t *testing.T) {
	ctx := context.Background()
	st := openNoteStatusAPIStore(t)
	defer st.Close()
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	unspentID := txidForTest('a') + ":0"
	pendingID := txidForTest('b') + ":1"
	spentID := txidForTest('c') + ":2"
	unknownID := txidForTest('d') + ":3"
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: txidForTest('e')}); err != nil {
			return err
		}
		for i, note := range []store.Note{
			{WalletID: "hot", TxID: txidForTest('a'), ActionIndex: 0, Height: 1, ValueZat: 100, NoteNullifier: txidForTest('1')},
			{WalletID: "hot", TxID: txidForTest('b'), ActionIndex: 1, Height: 2, ValueZat: 200, NoteNullifier: txidForTest('2')},
			{WalletID: "hot", TxID: txidForTest('c'), ActionIndex: 2, Height: 3, ValueZat: 300, NoteNullifier: txidForTest('3')},
		} {
			position := int64(i)
			note.Position = &position
			if _, err := tx.InsertNote(ctx, note); err != nil {
				return err
			}
		}
		_, err := tx.MarkNotesSpent(ctx, 9, txidForTest('f'), []string{txidForTest('3')})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	expiry := int64(25)
	seenAt := time.Unix(1_700_000_000, 0).UTC()
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{
		txidForTest('2'): {TxID: txidForTest('9'), ExpiryHeight: &expiry},
	}, 10, seenAt); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		_, err := tx.ConfirmSpends(ctx, 10, 9)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	server, err := New(st, WithBearerToken("scanner-secret"), currentMempoolRefreshOption(t, st))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(apiWalletNoteStatusRequest{NoteIDs: []string{unknownID, pendingID, unspentID, spentID}})
	req := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer scanner-secret")
	rr := httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var got apiWalletNoteStatusesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.WalletID != "hot" || len(got.EventEpoch) != store.EventEpochHexLength || got.AsOfScannerHeight != 10 || got.AsOfScannerHash != txidForTest('e') || len(got.Statuses) != 4 {
		t.Fatalf("response=%+v", got)
	}
	if got.Statuses[0].NoteID != unknownID || got.Statuses[0].State != "unknown" || got.Statuses[0].SourceHeight != nil || got.Statuses[0].ValueZat != nil {
		t.Fatalf("unknown=%+v", got.Statuses[0])
	}
	if item := got.Statuses[1]; item.NoteID != pendingID || item.State != "pending" || item.SourceHeight == nil || *item.SourceHeight != 2 || item.ValueZat == nil || *item.ValueZat != 200 || item.PendingSpentTxID == nil || *item.PendingSpentTxID != txidForTest('9') || item.PendingSpentAt == nil || !item.PendingSpentAt.Equal(seenAt) || item.PendingSpentExpiryHeight == nil || *item.PendingSpentExpiryHeight != expiry {
		t.Fatalf("pending=%+v", item)
	}
	if item := got.Statuses[2]; item.NoteID != unspentID || item.State != "unspent" || item.SourceHeight == nil || *item.SourceHeight != 1 || item.ValueZat == nil || *item.ValueZat != 100 {
		t.Fatalf("unspent=%+v", item)
	}
	if item := got.Statuses[3]; item.NoteID != spentID || item.State != "spent" || item.SourceHeight == nil || *item.SourceHeight != 3 || item.ValueZat == nil || *item.ValueZat != 300 || item.SpentTxID == nil || *item.SpentTxID != txidForTest('f') || item.SpentHeight == nil || *item.SpentHeight != 9 || item.SpentConfirmedHeight == nil || *item.SpentConfirmedHeight != 10 {
		t.Fatalf("spent=%+v", item)
	}
	assertNoteStatusResponseFields(t, rr.Body.Bytes())

	unauthorizedReq := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", bytes.NewReader(body))
	unauthorizedReq.Header.Set("Content-Type", "application/json")
	unauthorized := httptest.NewRecorder()
	server.Handler().ServeHTTP(unauthorized, unauthorizedReq)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
}

func TestWalletNoteStatusesEndpointStrictValidation(t *testing.T) {
	st := openNoteStatusAPIStore(t)
	defer st.Close()
	if err := st.UpsertWallet(context.Background(), "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(context.Background(), func(tx store.Tx) error {
		return tx.InsertBlock(context.Background(), store.Block{Height: 10, Hash: txidForTest('e')})
	}); err != nil {
		t.Fatal(err)
	}
	server, err := New(st, currentMempoolRefreshOption(t, st))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	valid := txidForTest('a') + ":0"
	tooMany := make([]string, store.MaxNoteStatusBatch+1)
	for i := range tooMany {
		tooMany[i] = txidForTest('b') + ":" + strconv.Itoa(i)
	}
	cases := []struct {
		name        string
		body        string
		contentType string
		want        int
	}{
		{name: "missing content type", body: `{"note_ids":["` + valid + `"]}`, want: http.StatusBadRequest},
		{name: "wrong content type", body: `{"note_ids":["` + valid + `"]}`, contentType: "text/plain", want: http.StatusBadRequest},
		{name: "malformed", body: `{`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "unknown field", body: `{"note_ids":["` + valid + `"],"extra":true}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "trailing json", body: `{"note_ids":["` + valid + `"]}{}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "empty", body: `{"note_ids":[]}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "uppercase", body: `{"note_ids":["` + strings.ToUpper(txidForTest('a')) + `:0"]}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "leading zero", body: `{"note_ids":["` + txidForTest('a') + `:01"]}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "uint32 overflow", body: `{"note_ids":["` + txidForTest('a') + `:4294967296"]}`, contentType: "application/json", want: http.StatusBadRequest},
		{name: "duplicate", body: `{"note_ids":["` + valid + `","` + valid + `"]}`, contentType: "application/json", want: http.StatusBadRequest},
	}
	tooManyBody, _ := json.Marshal(apiWalletNoteStatusRequest{NoteIDs: tooMany})
	cases = append(cases, struct {
		name        string
		body        string
		contentType string
		want        int
	}{name: "too many", body: string(tooManyBody), contentType: "application/json", want: http.StatusBadRequest})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != tc.want {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
	maxBatch := tooMany[:store.MaxNoteStatusBatch]
	maxBatchBody, _ := json.Marshal(apiWalletNoteStatusRequest{NoteIDs: maxBatch})
	req := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", bytes.NewReader(maxBatchBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("max batch status=%d body=%s", rr.Code, rr.Body.String())
	}
	var maxBatchResponse apiWalletNoteStatusesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &maxBatchResponse); err != nil || len(maxBatchResponse.Statuses) != store.MaxNoteStatusBatch {
		t.Fatalf("max batch response count=%d err=%v", len(maxBatchResponse.Statuses), err)
	}

	maxUintBody := `{"note_ids":["` + txidForTest('c') + `:4294967295"]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(maxUintBody))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("max uint32 status=%d body=%s", rr.Code, rr.Body.String())
	}

	large := `{"note_ids":["` + valid + `"],"padding":"` + strings.Repeat("x", walletNoteStatusMaxBodyBytes) + `"}`
	req = httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(large))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large status=%d body=%s", rr.Code, rr.Body.String())
	}
	chunkedLarge := `{"note_ids":["` + strings.Repeat("a", walletNoteStatusMaxBodyBytes) + `"]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(chunkedLarge))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked large status=%d body=%s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/wallets/hot/notes/status", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWalletNoteStatusesEndpointFailures(t *testing.T) {
	ctx := context.Background()
	ready := openNoteStatusAPIStore(t)
	defer ready.Close()
	if err := ready.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if err := ready.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 10, Hash: txidForTest('e')})
	}); err != nil {
		t.Fatal(err)
	}
	validBody := `{"note_ids":["` + txidForTest('a') + `:0"]}`
	refreshStatus := currentMempoolRefreshStatus(t, ready)
	do := func(t *testing.T, st store.Store) *httptest.ResponseRecorder {
		t.Helper()
		status := refreshStatus
		if _, ok := st.(*invalidHashNoteStatusStore); ok {
			status.Hash = "not-a-canonical-hash"
		}
		server, err := New(st, WithMempoolRefreshStatus(func() MempoolRefreshStatus { return status }))
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		return rr
	}

	if rr := do(t, ready); rr.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", rr.Code, rr.Body.String())
	}
	server, err := New(ready, WithMempoolRefreshStatus(func() MempoolRefreshStatus { return refreshStatus }))
	if err != nil {
		t.Fatal(err)
	}
	missingReq := httptest.NewRequest(http.MethodPost, "/v1/wallets/missing/notes/status", strings.NewReader(validBody))
	missingReq.Header.Set("Content-Type", "application/json")
	missingRR := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingRR, missingReq)
	if missingRR.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%s", missingRR.Code, missingRR.Body.String())
	}

	notReady := openNoteStatusAPIStore(t)
	defer notReady.Close()
	if err := notReady.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if rr := do(t, notReady); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("not ready status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, &flappingTipStore{Store: ready}); rr.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, &singleChangeTipStore{Store: ready}); rr.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, &failingNoteStatusStore{Store: ready}); rr.Code != http.StatusInternalServerError {
		t.Fatalf("db error status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, &stalePendingNoteStatusStore{Store: ready}); rr.Code != http.StatusInternalServerError {
		t.Fatalf("stale pending status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, &invalidHashNoteStatusStore{Store: ready}); rr.Code != http.StatusInternalServerError {
		t.Fatalf("invalid hash status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWalletNoteStatusesEndpointRequiresCurrentMempoolRefresh(t *testing.T) {
	ctx := context.Background()
	st := openNoteStatusAPIStore(t)
	defer st.Close()
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertBlock(ctx, store.Block{Height: 10, Hash: txidForTest('e')})
	}); err != nil {
		t.Fatal(err)
	}

	current := currentMempoolRefreshStatus(t, st)
	refresh := MempoolRefreshStatus{}
	server, err := New(st, WithMempoolRefreshStatus(func() MempoolRefreshStatus { return refresh }))
	if err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/wallets/hot/notes/status", strings.NewReader(`{"note_ids":["`+txidForTest('a')+`:0"]}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		return rr
	}

	if rr := request(); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("startup status=%d body=%s", rr.Code, rr.Body.String())
	}
	refresh = current
	refresh.EventEpoch = txidForTest('f')
	if rr := request(); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("epoch mismatch status=%d body=%s", rr.Code, rr.Body.String())
	}
	refresh = current
	refresh.Height--
	if rr := request(); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("tip mismatch status=%d body=%s", rr.Code, rr.Body.String())
	}
	refresh = current
	if rr := request(); rr.Code != http.StatusOK {
		t.Fatalf("current refresh status=%d body=%s", rr.Code, rr.Body.String())
	}
}

type flappingTipStore struct {
	store.Store
	calls atomic.Int32
}

type singleChangeTipStore struct {
	store.Store
	calls atomic.Int32
}

func (s *singleChangeTipStore) Tip(context.Context) (store.BlockTip, bool, error) {
	if s.calls.Add(1) == 1 {
		return store.BlockTip{Height: 9, Hash: txidForTest('d')}, true, nil
	}
	return store.BlockTip{Height: 10, Hash: txidForTest('e')}, true, nil
}

func (s *flappingTipStore) Tip(context.Context) (store.BlockTip, bool, error) {
	if s.calls.Add(1)%2 == 0 {
		return store.BlockTip{Height: 11, Hash: txidForTest('f')}, true, nil
	}
	return store.BlockTip{Height: 10, Hash: txidForTest('e')}, true, nil
}

type failingNoteStatusStore struct{ store.Store }

func (s *failingNoteStatusStore) WalletNoteStatuses(context.Context, string, []store.NoteRef) (store.WalletNoteStatusSnapshot, error) {
	return store.WalletNoteStatusSnapshot{}, errors.New("injected failure")
}

type stalePendingNoteStatusStore struct{ store.Store }

func (s *stalePendingNoteStatusStore) WalletNoteStatuses(ctx context.Context, walletID string, refs []store.NoteRef) (store.WalletNoteStatusSnapshot, error) {
	snapshot, err := s.Store.WalletNoteStatuses(ctx, walletID, refs)
	if err != nil || len(refs) == 0 {
		return snapshot, err
	}
	pendingTxID := txidForTest('9')
	pendingAt := time.Unix(1_700_000_000, 0).UTC()
	staleExpiry := snapshot.AsOfScannerHeight - 1
	snapshot.Notes[refs[0]] = store.Note{
		WalletID:                 walletID,
		TxID:                     refs[0].TxID,
		ActionIndex:              int32(refs[0].ActionIndex),
		Height:                   1,
		ValueZat:                 1,
		PendingSpentTxID:         &pendingTxID,
		PendingSpentAt:           &pendingAt,
		PendingSpentExpiryHeight: &staleExpiry,
	}
	return snapshot, nil
}

type invalidHashNoteStatusStore struct{ store.Store }

func (s *invalidHashNoteStatusStore) Tip(context.Context) (store.BlockTip, bool, error) {
	return store.BlockTip{Height: 10, Hash: "not-a-canonical-hash"}, true, nil
}

func (s *invalidHashNoteStatusStore) WalletNoteStatuses(ctx context.Context, walletID string, refs []store.NoteRef) (store.WalletNoteStatusSnapshot, error) {
	snapshot, err := s.Store.WalletNoteStatuses(ctx, walletID, refs)
	snapshot.AsOfScannerHash = "not-a-canonical-hash"
	return snapshot, err
}

func openNoteStatusAPIStore(t *testing.T) *rocksdb.Store {
	t.Helper()
	st, err := rocksdb.Open(t.TempDir() + "/scan.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		st.Close()
		t.Fatal(err)
	}
	return st
}

func assertNoteStatusResponseFields(t *testing.T, body []byte) {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	if len(root) != 5 {
		t.Fatalf("response fields=%v", root)
	}
	for _, key := range []string{"wallet_id", "event_epoch", "as_of_scanner_height", "as_of_scanner_hash", "statuses"} {
		if _, ok := root[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	var statuses []map[string]json.RawMessage
	if err := json.Unmarshal(root["statuses"], &statuses); err != nil {
		t.Fatal(err)
	}
	wantFields := [][]string{
		{"note_id", "state"},
		{"note_id", "state", "source_height", "value_zat", "pending_spent_txid", "pending_spent_at", "pending_spent_expiry_height"},
		{"note_id", "state", "source_height", "value_zat"},
		{"note_id", "state", "source_height", "value_zat", "spent_txid", "spent_height", "spent_confirmed_height"},
	}
	for i, want := range wantFields {
		if len(statuses[i]) != len(want) {
			t.Fatalf("status[%d] fields=%v want=%v", i, statuses[i], want)
		}
		for _, key := range want {
			if _, ok := statuses[i][key]; !ok {
				t.Fatalf("status[%d] missing %s", i, key)
			}
		}
	}
}
