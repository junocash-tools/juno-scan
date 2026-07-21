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

func TestAddressBalanceAndRecipientFilter(t *testing.T) {
	ctx := context.Background()
	st, err := rocksdb.Open(t.TempDir() + "/scan.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "exchange", "ufvk-test"); err != nil {
		t.Fatal(err)
	}
	address := "jregtest1testaddress"
	other := "jregtest1other"
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 10, Hash: "hash-10"}); err != nil {
			return err
		}
		notes := []store.Note{
			{WalletID: "exchange", TxID: txidForTest('a'), ActionIndex: 0, Height: 1, RecipientAddress: address, ValueZat: 100, NoteNullifier: txidForTest('1')},
			{WalletID: "exchange", TxID: txidForTest('b'), ActionIndex: 0, Height: 9, RecipientAddress: address, ValueZat: 50, NoteNullifier: txidForTest('2')},
			{WalletID: "exchange", TxID: txidForTest('c'), ActionIndex: 0, Height: 9, RecipientAddress: address, ValueZat: 30, NoteNullifier: txidForTest('3')},
			{WalletID: "exchange", TxID: txidForTest('d'), ActionIndex: 0, Height: 1, RecipientAddress: other, ValueZat: 999, NoteNullifier: txidForTest('4')},
		}
		for _, note := range notes {
			if _, err := tx.InsertNote(ctx, note); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdatePendingSpends(ctx, map[string]store.PendingSpend{txidForTest('3'): {TxID: txidForTest('e')}}, 10, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	server, err := New(st, WithRuntimeStatus("regtest", "jregtest", 5, 2, func() (int64, bool) { return 10, true }))
	if err != nil {
		t.Fatal(err)
	}
	healthReq := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	healthRR := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthRR, healthReq)
	var health struct {
		EventEpoch string `json:"event_epoch"`
	}
	if healthRR.Code != http.StatusOK || json.Unmarshal(healthRR.Body.Bytes(), &health) != nil || len(health.EventEpoch) != store.EventEpochHexLength {
		t.Fatalf("health epoch status=%d body=%s", healthRR.Code, healthRR.Body.String())
	}
	eventsReq := httptest.NewRequest(http.MethodGet, "/v1/wallets/exchange/events", nil)
	eventsRR := httptest.NewRecorder()
	server.Handler().ServeHTTP(eventsRR, eventsReq)
	var eventPage struct {
		EventEpoch string `json:"event_epoch"`
	}
	if eventsRR.Code != http.StatusOK || json.Unmarshal(eventsRR.Body.Bytes(), &eventPage) != nil || eventPage.EventEpoch != health.EventEpoch {
		t.Fatalf("event page epoch status=%d body=%s", eventsRR.Code, eventsRR.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/wallets/exchange/addresses/"+address+"/balance?min_confirmations=5", nil)
	rr := httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got struct {
		WalletID           string `json:"wallet_id"`
		RecipientAddress   string `json:"recipient_address"`
		MinConfirmations   int64  `json:"min_confirmations"`
		AvailableZat       int64  `json:"available_zat"`
		PendingIncomingZat int64  `json:"pending_incoming_zat"`
		PendingOutgoingZat int64  `json:"pending_outgoing_zat"`
		TotalUnspentZat    int64  `json:"total_unspent_zat"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.AvailableZat != 100 || got.PendingIncomingZat != 50 || got.PendingOutgoingZat != 30 || got.TotalUnspentZat != 180 {
		t.Fatalf("unexpected balance: %+v", got)
	}
	if got.AvailableZat+got.PendingIncomingZat+got.PendingOutgoingZat != got.TotalUnspentZat {
		t.Fatalf("overlapping balance buckets: %+v", got)
	}
	if got.WalletID != "exchange" || got.RecipientAddress != address || got.MinConfirmations != 5 {
		t.Fatalf("unexpected response identity: %+v", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/exchange/addresses/"+address+"/balance?min_confirmations=0", nil)
	rr = httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("min_confirmations=0 status=%d body=%s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.MinConfirmations != 0 || got.AvailableZat != 150 || got.PendingIncomingZat != 0 || got.PendingOutgoingZat != 30 {
		t.Fatalf("unexpected zero-confirmation balance: %+v", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/exchange/addresses/jregtest1unused/balance", nil)
	rr = httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("zero balance status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/wallets/missing/addresses/jregtest1unused/balance", nil)
	rr = httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing wallet status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func txidForTest(ch byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = ch
	}
	return string(b)
}
