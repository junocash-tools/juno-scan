//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/postgres"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/jackc/pgx/v5"
)

func TestPostgresRollbackConcurrencyExpiryAndShardParity(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("JUNO_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}
	tests := []struct {
		name string
		run  func(*testing.T, context.Context, store.Store)
	}{
		{"canonical writer interleaving", exerciseRollbackWaitsForCanonicalWriter},
		{"expiry replay", exerciseOutgoingExpiryRollbackReplay},
		{"shard cache rewind", exerciseShardCacheRollback},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			schema := fmt.Sprintf("junoscan_reorg_%d", time.Now().UnixNano())
			st, err := postgres.Open(ctx, dsn, schema)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			defer dropPostgresSchema(t, ctx, dsn, schema)
			tc.run(t, ctx, st)
		})
	}
}

func TestPostgresDestructiveResetPreservesWalletAndRotatesCursorEpoch(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("JUNO_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	schema := fmt.Sprintf("junoscan_reset_%d", time.Now().UnixNano())
	st, err := postgres.Open(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	defer dropPostgresSchema(t, ctx, dsn, schema)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok {
		t.Fatalf("progress=%+v ok=%v err=%v", progress, ok, err)
	}
	oldGeneration := progress.Generation
	progress.NextHeight, progress.TargetHeight, progress.State = 101, 100, "complete"
	if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
		t.Fatal(err)
	}
	oldEpoch, err := st.EventEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seenAt := time.Unix(100, 0).UTC()
	expiryHeight := int64(10)
	pendingTxID := strings.Repeat("f", 64)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{Height: 1, Hash: "h1"}); err != nil {
			return err
		}
		if _, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{WalletID: "exchange", TxID: pendingTxID, ActionIndex: 0, MempoolSeenAt: &seenAt, TxExpiryHeight: &expiryHeight, RecipientAddress: "u", ValueZat: 1, OvkScope: "external"}); err != nil {
			return err
		}
		if err := tx.InsertEvent(ctx, store.Event{Kind: "OutgoingOutputEvent", WalletID: "exchange", Height: 0, Payload: json.RawMessage(`{"txid":"` + pendingTxID + `"}`)}); err != nil {
			return err
		}
		return tx.InsertEvent(ctx, store.Event{Kind: "DepositEvent", WalletID: "exchange", Height: 1, Payload: json.RawMessage(`{"txid":"tx1"}`)})
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetWalletEventPublishCursor(ctx, "exchange", 2); err != nil {
		t.Fatal(err)
	}
	if maxID, err := st.MaxWalletEventID(ctx, "exchange"); err != nil || maxID <= 0 {
		t.Fatalf("postgres max event id=%d err=%v", maxID, err)
	}
	if err := st.RollbackToHeight(ctx, -1); err != nil {
		t.Fatal(err)
	}
	newEpoch, err := st.EventEpoch(ctx)
	if err != nil || newEpoch == oldEpoch {
		t.Fatalf("epoch did not rotate: old=%q new=%q err=%v", oldEpoch, newEpoch, err)
	}
	progress, ok, err = st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 50 || progress.NextHeight != 50 || progress.State != "pending" || progress.Generation <= oldGeneration {
		t.Fatalf("progress not reset: %+v ok=%v err=%v", progress, ok, err)
	}
	if wallets, err := st.ListWallets(ctx); err != nil || len(wallets) != 1 || wallets[0].WalletID != "exchange" || wallets[0].BirthdayHeight != 50 {
		t.Fatalf("wallet identity not preserved: %+v err=%v", wallets, err)
	}
	if _, ok, err := st.Tip(ctx); err != nil || ok {
		t.Fatalf("chain survived destructive reset: ok=%v err=%v", ok, err)
	}
	if maxID, err := st.MaxWalletEventID(ctx, "exchange"); err != nil || maxID != 0 {
		t.Fatalf("postgres empty max event id=%d err=%v", maxID, err)
	}
	if cursor, err := st.WalletEventPublishCursor(ctx, "exchange"); err != nil || cursor != 0 {
		t.Fatalf("postgres publish cursor survived reset: cursor=%d err=%v", cursor, err)
	}
	if outputs, err := st.ListOutgoingOutputsByTxID(ctx, "exchange", pendingTxID); err != nil || len(outputs) != 0 {
		t.Fatalf("postgres mempool output survived reset: %+v err=%v", outputs, err)
	}
	changed := false
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		var err error
		changed, err = tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{WalletID: "exchange", TxID: pendingTxID, ActionIndex: 0, MempoolSeenAt: &seenAt, TxExpiryHeight: &expiryHeight, RecipientAddress: "u", ValueZat: 1, OvkScope: "external"})
		if err != nil || !changed {
			return err
		}
		return tx.InsertEvent(ctx, store.Event{Kind: "OutgoingOutputEvent", WalletID: "exchange", Height: 0, Payload: json.RawMessage(`{"txid":"` + pendingTxID + `"}`)})
	}); err != nil || !changed {
		t.Fatalf("postgres mempool output was not replayable: changed=%v err=%v", changed, err)
	}
}

func TestPostgresUpgradeFromV14SeedsBackfillGeneration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("JUNO_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	schema := fmt.Sprintf("junoscan_upgrade_%d", time.Now().UnixNano())
	st, err := postgres.Open(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	defer dropPostgresSchema(t, ctx, dsn, schema)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM wallet_backfill_progress; DELETE FROM schema_migrations WHERE version=15; ALTER TABLE wallet_backfill_progress DROP COLUMN generation`); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 50 || progress.NextHeight != 50 || progress.State != "pending" || progress.Generation != 1 {
		t.Fatalf("v14 upgrade progress=%+v ok=%v err=%v", progress, ok, err)
	}
	if err := st.UpsertWallet(ctx, "other", "ufvk"); !errors.Is(err, store.ErrUFVKAlreadyRegistered) {
		t.Fatalf("v14 unique invariant err=%v", err)
	}
}

func TestPostgresUpgradeFromV15RecoversLegacyExpiryObservation(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("JUNO_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	schema := fmt.Sprintf("junoscan_expiry_upgrade_%d", time.Now().UnixNano())
	st, err := postgres.Open(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	defer dropPostgresSchema(t, ctx, dsn, schema)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	seenAt := time.Unix(100, 0).UTC()
	expiryHeight := int64(5)
	txWithEvent, txWithoutEvent := strings.Repeat("d", 64), strings.Repeat("e", 64)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		for _, txid := range []string{txWithEvent, txWithoutEvent} {
			if _, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{WalletID: "hot", TxID: txid, ActionIndex: 0, MempoolSeenAt: &seenAt, TxExpiryHeight: &expiryHeight, RecipientAddress: "u", ValueZat: 1, OvkScope: "external"}); err != nil {
				return err
			}
		}
		if expired, err := tx.ExpireOutgoingOutputs(ctx, 6, time.Now().UTC()); err != nil || len(expired) != 2 {
			return fmt.Errorf("expire legacy fixtures: count=%d err=%w", len(expired), err)
		}
		return tx.InsertEvent(ctx, store.Event{Kind: "OutgoingOutputExpired", WalletID: "hot", Height: 6, Payload: json.RawMessage(`{"txid":"` + txWithEvent + `","action_index":0}`)})
	}); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SET search_path TO `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM schema_migrations WHERE version=16; ALTER TABLE outgoing_outputs DROP COLUMN expired_height`); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for txid, want := range map[string]int64{txWithEvent: 6, txWithoutEvent: int64(1<<63 - 1)} {
		var got int64
		if err := conn.QueryRow(ctx, `SELECT expired_height FROM outgoing_outputs WHERE wallet_id='hot' AND txid=$1 AND action_index=0`, txid).Scan(&got); err != nil || got != want {
			t.Fatalf("legacy expiry txid=%s height=%d want=%d err=%v", txid[:1], got, want, err)
		}
	}
	if err := st.RollbackToHeight(ctx, 5); err != nil {
		t.Fatal(err)
	}
	for _, txid := range []string{txWithEvent, txWithoutEvent} {
		outputs, err := st.ListOutgoingOutputsByTxID(ctx, "hot", txid)
		if err != nil || len(outputs) != 1 || outputs[0].ExpiredAt != nil {
			t.Fatalf("legacy expiry did not unexpire txid=%s outputs=%+v err=%v", txid[:1], outputs, err)
		}
	}
}

func TestScanner_DepositDetected_Postgres(t *testing.T) {
	dsn := os.Getenv("JUNO_TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("JUNO_TEST_POSTGRES_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	deposit := waitForEventKind(t, ctx, st, "hot", "DepositEvent")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	confirmed := waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	if mustTxIDFromPayload(t, confirmed.Payload) != mustTxIDFromPayload(t, deposit.Payload) {
		t.Fatalf("confirmed txid mismatch")
	}

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend.
	toAddr := mustCreateUnifiedAddress(t, ctx, jd)
	opid2 := mustSendMany(t, ctx, jd, addr, toAddr, "0.01")
	mustWaitOpSuccess(t, ctx, jd, opid2)
	spendTxID := mustTxIDForOpID(t, ctx, jd, opid2)
	waitForPendingSpend(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventState(t, ctx, st, "hot", spendTxID, "mempool")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendEvent")
	waitForOutgoingOutputEventState(t, ctx, st, "hot", spendTxID, "confirmed")
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	waitForEventKind(t, ctx, st, "hot", "SpendConfirmed")
	waitForEventKind(t, ctx, st, "hot", "OutgoingOutputConfirmed")
	waitForOutgoingOutputPageEntry(t, ctx, st, "hot", spendTxID, toAddr)
	events, _, err := st.ListWalletEvents(ctx, "hot", 0, 1000, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListWalletEvents: %v", err)
	}
	depositEvents, depositConfirmed := 0, 0
	for _, event := range events {
		switch event.Kind {
		case "DepositEvent":
			depositEvents++
		case "DepositConfirmed":
			depositConfirmed++
		}
	}
	if depositEvents != 1 || depositConfirmed != 1 {
		t.Fatalf("withdrawal change emitted deposit lifecycle: DepositEvent=%d DepositConfirmed=%d", depositEvents, depositConfirmed)
	}

	notesAll, err := st.ListWalletNotes(ctx, "hot", false, 1000)
	if err != nil {
		t.Fatalf("ListWalletNotes(all): %v", err)
	}
	foundSpent := false
	for _, n := range notesAll {
		if n.SpentTxID != nil && strings.TrimSpace(*n.SpentTxID) == spendTxID {
			foundSpent = true
			if n.PendingSpentTxID != nil {
				t.Fatalf("pending_spent_txid not cleared")
			}
			if n.PendingSpentAt != nil {
				t.Fatalf("pending_spent_at not cleared")
			}
			if n.PendingSpentExpiryHeight != nil {
				t.Fatalf("pending_spent_expiry_height not cleared")
			}
		}
	}
	if !foundSpent {
		t.Fatalf("spent note not found")
	}
}

func dropPostgresSchema(t *testing.T, ctx context.Context, dsn string, schema string) {
	t.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	_, _ = conn.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
}
