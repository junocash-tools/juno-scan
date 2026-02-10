//go:build integration && docker

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/postgres"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_MempoolSpendExpires_RocksDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	addr, ufvk := mustCreateWalletAndUFVK(t, ctx, jd)
	uaHRP := strings.SplitN(addr, "1", 2)[0]

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := st.UpsertWallet(ctx, "hot", ufvk); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	sc, err := scanner.New(st, rpc, uaHRP, 100*time.Millisecond, 2, "")
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	scErrCh := make(chan error, 1)
	go func() { scErrCh <- sc.Run(runCtx) }()
	defer func() {
		runCancel()
		select {
		case <-scErrCh:
		case <-time.After(5 * time.Second):
		}
	}()

	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	waitForEventKind(t, ctx, st, "hot", "DepositEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend with a low fee so the tx enters the mempool but is not mined (see docker stack flags).
	to1 := mustCreateUnifiedAddress(t, ctx, jd)
	to2 := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendManyWithFee(t, ctx, jd, addr, []string{to1, to2}, []string{"0.01", "0.01"}, "1", "0.0001")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)

	expiryHeight := waitForPendingSpendWithExpiryHeight(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventMempoolWithExpiryHeight(t, ctx, st, "hot", spendTxID, expiryHeight)

	// Mine blocks until the tx is expired (chainHeight > expiry_height).
	tip, err := rpc.GetBlockCount(ctx)
	if err != nil {
		t.Fatalf("getblockcount: %v", err)
	}
	blocksToMine := (expiryHeight + 1) - tip
	if blocksToMine < 1 {
		blocksToMine = 1
	}
	mustRun(t, jd.CLICommand(ctx, "generate", strconv.FormatInt(blocksToMine, 10)))

	waitForOutgoingOutputExpiredEvent(t, ctx, st, "hot", spendTxID, expiryHeight)
	waitForPendingSpendCleared(t, ctx, st, "hot", spendTxID)
}

func TestScanner_MempoolSpendExpires_Postgres(t *testing.T) {
	dsn := os.Getenv("JUNO_TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	schema := fmt.Sprintf("junoscan_test_%d", time.Now().UnixNano())
	st, err := postgres.Open(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	defer dropPostgresSchema(t, ctx, dsn, schema)

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	addr, ufvk := mustCreateWalletAndUFVK(t, ctx, jd)
	uaHRP := strings.SplitN(addr, "1", 2)[0]

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := st.UpsertWallet(ctx, "hot", ufvk); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	rpc := sdkjunocashd.New(jd.RPCURL, jd.RPCUser, jd.RPCPassword)
	sc, err := scanner.New(st, rpc, uaHRP, 100*time.Millisecond, 2, "")
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	scErrCh := make(chan error, 1)
	go func() { scErrCh <- sc.Run(runCtx) }()
	defer func() {
		runCancel()
		select {
		case <-scErrCh:
		case <-time.After(5 * time.Second):
		}
	}()

	mustRun(t, jd.CLICommand(ctx, "generate", "101"))
	fromAddr := mustCoinbaseAddress(t, ctx, jd)
	opid := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	waitForEventKind(t, ctx, st, "hot", "DepositEvent")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	to1 := mustCreateUnifiedAddress(t, ctx, jd)
	to2 := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendManyWithFee(t, ctx, jd, addr, []string{to1, to2}, []string{"0.01", "0.01"}, "1", "0.0001")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)

	expiryHeight := waitForPendingSpendWithExpiryHeight(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventMempoolWithExpiryHeight(t, ctx, st, "hot", spendTxID, expiryHeight)

	tip, err := rpc.GetBlockCount(ctx)
	if err != nil {
		t.Fatalf("getblockcount: %v", err)
	}
	blocksToMine := (expiryHeight + 1) - tip
	if blocksToMine < 1 {
		blocksToMine = 1
	}
	mustRun(t, jd.CLICommand(ctx, "generate", strconv.FormatInt(blocksToMine, 10)))

	waitForOutgoingOutputExpiredEvent(t, ctx, st, "hot", spendTxID, expiryHeight)
	waitForPendingSpendCleared(t, ctx, st, "hot", spendTxID)
}

func mustSendManyWithFee(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr string, toAddrs []string, amounts []string, minconf string, fee string) string {
	t.Helper()
	if len(toAddrs) != len(amounts) || len(toAddrs) == 0 {
		t.Fatalf("toAddrs/amounts mismatch")
	}

	var b strings.Builder
	b.WriteString("[")
	for i := range toAddrs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"address":"`)
		b.WriteString(toAddrs[i])
		b.WriteString(`","amount":`)
		b.WriteString(amounts[i])
		b.WriteString("}")
	}
	b.WriteString("]")

	out := mustRun(t, jd.CLICommand(ctx, "z_sendmany", fromAddr, b.String(), strings.TrimSpace(minconf), strings.TrimSpace(fee)))

	var resp struct {
		OpID string `json:"opid"`
	}
	if err := json.Unmarshal(out, &resp); err == nil && resp.OpID != "" {
		return resp.OpID
	}
	opid := strings.TrimSpace(string(out))
	if opid == "" {
		t.Fatalf("missing opid")
	}
	return opid
}

func waitForPendingSpendWithExpiryHeight(t *testing.T, ctx context.Context, st store.Store, walletID string, spendTxID string) int64 {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		notes, err := st.ListWalletNotes(ctx, walletID, true, 1000)
		if err == nil {
			for _, n := range notes {
				if n.PendingSpentTxID != nil && strings.TrimSpace(*n.PendingSpentTxID) == spendTxID {
					if n.PendingSpentAt == nil {
						t.Fatalf("pending_spent_at not set")
					}
					if n.PendingSpentExpiryHeight == nil || *n.PendingSpentExpiryHeight <= 0 {
						t.Fatalf("pending_spent_expiry_height not set")
					}
					return *n.PendingSpentExpiryHeight
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("pending spend not detected for txid=%s", spendTxID)
	return 0
}

func waitForPendingSpendCleared(t *testing.T, ctx context.Context, st store.Store, walletID string, spendTxID string) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		notes, err := st.ListWalletNotes(ctx, walletID, true, 1000)
		if err == nil {
			stillPending := false
			for _, n := range notes {
				if n.PendingSpentTxID != nil && strings.TrimSpace(*n.PendingSpentTxID) == spendTxID {
					stillPending = true
					break
				}
			}
			if !stillPending {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("pending spend not cleared for txid=%s", spendTxID)
}

func waitForOutgoingOutputEventMempoolWithExpiryHeight(t *testing.T, ctx context.Context, st store.Store, walletID string, txid string, wantExpiryHeight int64) {
	t.Helper()

	txid = strings.ToLower(strings.TrimSpace(txid))

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID         string `json:"txid"`
		ExpiryHeight *int64 `json:"expiry_height,omitempty"`
		Height       *int64 `json:"height,omitempty"`
		Status       struct {
			State string `json:"state"`
		} `json:"status"`
	}

	filter := store.EventFilter{
		Kinds: []string{"OutgoingOutputEvent"},
		TxID:  txid,
	}

	for time.Now().Before(deadline) {
		events, _, err := st.ListWalletEvents(ctx, walletID, 0, 1000, filter)
		if err == nil {
			for _, e := range events {
				if e.Kind != "OutgoingOutputEvent" {
					continue
				}
				var p payload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					continue
				}
				if strings.TrimSpace(p.TxID) != txid {
					continue
				}
				if p.Status.State != "mempool" {
					continue
				}
				if p.Height != nil {
					t.Fatalf("expected no payload.height for mempool outgoing output")
				}
				if p.ExpiryHeight == nil || *p.ExpiryHeight != wantExpiryHeight {
					t.Fatalf("expected expiry_height=%d, got %+v", wantExpiryHeight, p.ExpiryHeight)
				}
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("OutgoingOutputEvent txid=%s state=mempool with expiry_height=%d not found", txid, wantExpiryHeight)
}

func waitForOutgoingOutputExpiredEvent(t *testing.T, ctx context.Context, st store.Store, walletID string, txid string, wantExpiryHeight int64) {
	t.Helper()

	txid = strings.ToLower(strings.TrimSpace(txid))

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID         string `json:"txid"`
		ExpiryHeight *int64 `json:"expiry_height,omitempty"`
		Height       *int64 `json:"height,omitempty"`
		Status       struct {
			State string `json:"state"`
		} `json:"status"`
	}

	filter := store.EventFilter{
		Kinds: []string{"OutgoingOutputExpired"},
		TxID:  txid,
	}

	for time.Now().Before(deadline) {
		events, _, err := st.ListWalletEvents(ctx, walletID, 0, 1000, filter)
		if err == nil {
			for _, e := range events {
				if e.Kind != "OutgoingOutputExpired" {
					continue
				}
				var p payload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					continue
				}
				if strings.TrimSpace(p.TxID) != txid {
					continue
				}
				if p.Status.State != "expired" {
					continue
				}
				if p.Height != nil {
					t.Fatalf("expected no payload.height for expired outgoing output")
				}
				if p.ExpiryHeight == nil || *p.ExpiryHeight != wantExpiryHeight {
					t.Fatalf("expected expiry_height=%d, got %+v", wantExpiryHeight, p.ExpiryHeight)
				}
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("OutgoingOutputExpired txid=%s expiry_height=%d not found", txid, wantExpiryHeight)
}
