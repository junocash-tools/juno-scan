//go:build integration && mysql

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/mysql"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestMySQLWalletNoteSummary(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, admin, dbName, cleanup := openMySQLTestStoreWithAdmin(t, ctx, rootDSN)
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	exerciseWalletNoteSummary(t, ctx, st)
	if _, err := admin.ExecContext(ctx, "UPDATE `"+dbName+"`.wallets SET disabled_at=NOW() WHERE wallet_id=?", "summary-wallet"); err != nil {
		t.Fatal(err)
	}
	disabled, err := st.WalletNoteSummary(ctx, "summary-wallet", 10, 100, 100)
	if err != nil || disabled != (store.WalletNoteSummary{}) {
		t.Fatalf("disabled summary=%+v err=%v", disabled, err)
	}
	if _, err := admin.ExecContext(ctx, "UPDATE `"+dbName+"`.wallets SET disabled_at=NULL WHERE wallet_id=?", "summary-wallet"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "UPDATE `"+dbName+"`.notes SET spent_confirmed_height=? WHERE wallet_id=? AND txid=? AND action_index=?", 100, "summary-wallet", summaryBackendID('a'), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WalletNoteSummary(ctx, "summary-wallet", 10, 100, 100); !errors.Is(err, store.ErrInvalidWalletNoteState) {
		t.Fatalf("confirmed spend without mined spend error=%v want ErrInvalidWalletNoteState", err)
	}
}

func TestMySQLWalletNoteStatuses(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	exerciseWalletNoteStatuses(t, ctx, st)
}

func TestMySQLRollbackConcurrencyExpiryAndShardParity(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
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
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
			defer cleanup()
			tc.run(t, ctx, st)
		})
	}
}

func TestMySQLDestructiveResetPreservesWalletAndRotatesCursorEpoch(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
	defer cleanup()
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
		t.Fatalf("mysql max event id=%d err=%v", maxID, err)
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
		t.Fatalf("mysql empty max event id=%d err=%v", maxID, err)
	}
	if cursor, err := st.WalletEventPublishCursor(ctx, "exchange"); err != nil || cursor != 0 {
		t.Fatalf("mysql publish cursor survived reset: cursor=%d err=%v", cursor, err)
	}
	if outputs, err := st.ListOutgoingOutputsByTxID(ctx, "exchange", pendingTxID); err != nil || len(outputs) != 0 {
		t.Fatalf("mysql mempool output survived reset: %+v err=%v", outputs, err)
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
		t.Fatalf("mysql mempool output was not replayable: changed=%v err=%v", changed, err)
	}
}

func TestMySQLUpgradeFromV14SeedsBackfillGeneration(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, admin, dbName, cleanup := openMySQLTestStoreWithAdmin(t, ctx, rootDSN)
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM `"+dbName+"`.wallet_backfill_progress"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM `"+dbName+"`.schema_migrations WHERE version=15"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER TABLE `"+dbName+"`.wallet_backfill_progress DROP COLUMN generation"); err != nil {
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

func TestMySQLUpgradeFromV15RecoversLegacyExpiryObservation(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, admin, dbName, cleanup := openMySQLTestStoreWithAdmin(t, ctx, rootDSN)
	defer cleanup()
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
	if _, err := admin.ExecContext(ctx, "DELETE FROM `"+dbName+"`.schema_migrations WHERE version=16"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER TABLE `"+dbName+"`.outgoing_outputs DROP COLUMN expired_height"); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for txid, want := range map[string]int64{txWithEvent: 6, txWithoutEvent: int64(1<<63 - 1)} {
		var got int64
		if err := admin.QueryRowContext(ctx, "SELECT expired_height FROM `"+dbName+"`.outgoing_outputs WHERE wallet_id='hot' AND txid=? AND action_index=0", txid).Scan(&got); err != nil || got != want {
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

func TestMySQLPartialWalletMigrationsResumeStatementwise(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, admin, dbName, cleanup := openMySQLTestStoreWithAdmin(t, ctx, rootDSN)
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 25); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM `"+dbName+"`.wallet_backfill_progress"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DELETE FROM `"+dbName+"`.schema_migrations WHERE version IN (12,14,15)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "DROP INDEX wallets_ufvk_unique_idx ON `"+dbName+"`.wallets"); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 25 || progress.NextHeight != 25 || progress.Generation != 1 {
		t.Fatalf("resumed migration progress=%+v ok=%v err=%v", progress, ok, err)
	}
	var versions int
	if err := admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+dbName+"`.schema_migrations WHERE version IN (12,14,15)").Scan(&versions); err != nil || versions != 3 {
		t.Fatalf("resumed versions=%d err=%v", versions, err)
	}
	if err := st.UpsertWallet(ctx, "other", "ufvk"); !errors.Is(err, store.ErrUFVKAlreadyRegistered) {
		t.Fatalf("resumed unique invariant err=%v", err)
	}
}

func TestMySQLLifecycleTransitionsAreSingleWinner(t *testing.T) {
	rootDSN := os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN")
	if strings.TrimSpace(rootDSN) == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: strings.Repeat("a", 64), Height: 1, RecipientAddress: "u", ValueZat: 1, NoteNullifier: strings.Repeat("1", 64)}); err != nil {
			return err
		}
		if _, err := tx.InsertNote(ctx, store.Note{WalletID: "hot", TxID: strings.Repeat("b", 64), Height: 10, RecipientAddress: "u", ValueZat: 1, NoteNullifier: strings.Repeat("2", 64)}); err != nil {
			return err
		}
		h := int64(1)
		_, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{WalletID: "hot", TxID: strings.Repeat("c", 64), ActionIndex: 0, MinedHeight: &h, RecipientAddress: "u", ValueZat: 1, OvkScope: "external"})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	runContended := func(name string, transition func(store.Tx) (int, error)) {
		t.Helper()
		firstLocked := make(chan struct{})
		release := make(chan struct{})
		results := make(chan int, 2)
		errs := make(chan error, 2)
		go func() {
			err := st.WithTx(ctx, func(tx store.Tx) error {
				count, err := transition(tx)
				if err != nil {
					return err
				}
				close(firstLocked)
				<-release
				results <- count
				return nil
			})
			errs <- err
		}()
		<-firstLocked
		go func() {
			err := st.WithTx(ctx, func(tx store.Tx) error {
				count, err := transition(tx)
				if err == nil {
					results <- count
				}
				return err
			})
			errs <- err
		}()
		time.Sleep(100 * time.Millisecond)
		close(release)
		if err := <-errs; err != nil {
			t.Fatalf("%s first transaction: %v", name, err)
		}
		if err := <-errs; err != nil {
			t.Fatalf("%s second transaction: %v", name, err)
		}
		if total := <-results + <-results; total != 1 {
			t.Fatalf("%s returned %d lifecycle winners, want 1", name, total)
		}
	}

	runContended("confirm notes", func(tx store.Tx) (int, error) {
		notes, err := tx.ConfirmNotes(ctx, 2, 1)
		return len(notes), err
	})
	runContended("mark spent", func(tx store.Tx) (int, error) {
		notes, err := tx.MarkNotesSpent(ctx, 2, strings.Repeat("d", 64), []string{strings.Repeat("2", 64)})
		return len(notes), err
	})
	runContended("confirm spends", func(tx store.Tx) (int, error) {
		notes, err := tx.ConfirmSpends(ctx, 3, 2)
		return len(notes), err
	})
	runContended("confirm outgoing", func(tx store.Tx) (int, error) {
		outputs, err := tx.ConfirmOutgoingOutputs(ctx, 2, 1)
		return len(outputs), err
	})
}

func TestMySQLIgnoresClientFoundRowsForDepositIdempotency(t *testing.T) {
	rootDSN := strings.TrimSpace(os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN"))
	if rootDSN == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}
	cfg, err := driver.ParseDSN(rootDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ClientFoundRows = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, cleanup := openMySQLTestStore(t, ctx, cfg.FormatDSN())
	defer cleanup()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "hot", "ufvk"); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "hot")
	if err != nil || !ok {
		t.Fatalf("progress=%+v ok=%v err=%v", progress, ok, err)
	}
	expected := progress.NextHeight
	progress.ExpectedNextHeight = &expected
	if err := st.SetWalletBackfillProgress(ctx, progress); err != nil {
		t.Fatalf("identical valid CAS reported conflict: %v", err)
	}
	note := store.Note{WalletID: "hot", TxID: strings.Repeat("a", 64), Height: 1, RecipientAddress: "u", ValueZat: 1, NoteNullifier: strings.Repeat("1", 64)}
	insert := func() (bool, error) {
		inserted := false
		err := st.WithTx(ctx, func(tx store.Tx) error {
			var err error
			inserted, err = tx.InsertNote(ctx, note)
			if err != nil || !inserted {
				return err
			}
			return tx.InsertEvent(ctx, store.Event{Kind: "DepositEvent", WalletID: "hot", Height: 1, Payload: json.RawMessage(`{"txid":"` + note.TxID + `"}`)})
		})
		return inserted, err
	}
	if inserted, err := insert(); err != nil || !inserted {
		t.Fatalf("first insert=%v err=%v", inserted, err)
	}
	if inserted, err := insert(); err != nil || inserted {
		t.Fatalf("duplicate insert=%v err=%v", inserted, err)
	}
	events, _, err := st.ListWalletEvents(ctx, "hot", 0, 100, store.EventFilter{})
	if err != nil || len(events) != 1 {
		t.Fatalf("duplicate deposit event: events=%+v err=%v", events, err)
	}
}

func TestScanner_DepositDetected_MySQL(t *testing.T) {
	rootDSN := os.Getenv("JUNO_TEST_MYSQL_ROOT_DSN")
	if strings.TrimSpace(rootDSN) == "" {
		t.Skip("JUNO_TEST_MYSQL_ROOT_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	st, cleanup := openMySQLTestStore(t, ctx, rootDSN)
	defer cleanup()

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

func openMySQLTestStore(t *testing.T, ctx context.Context, rootDSN string) (*mysql.Store, func()) {
	t.Helper()
	st, _, _, cleanup := openMySQLTestStoreWithAdmin(t, ctx, rootDSN)
	return st, cleanup
}

func openMySQLTestStoreWithAdmin(t *testing.T, ctx context.Context, rootDSN string) (*mysql.Store, *sql.DB, string, func()) {
	t.Helper()

	cfg, err := driver.ParseDSN(rootDSN)
	if err != nil {
		t.Fatalf("parse root dsn: %v", err)
	}
	if cfg.DBName == "" {
		t.Fatalf("root dsn must include a database name (e.g. /mysql)")
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping: %v", err)
	}

	dbName := fmt.Sprintf("junoscan_test_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE DATABASE `"+dbName+"`"); err != nil {
		_ = db.Close()
		t.Fatalf("create database: %v", err)
	}

	cfg2 := *cfg
	cfg2.DBName = dbName
	st, err := mysql.Open(ctx, cfg2.FormatDSN())
	if err != nil {
		_, _ = db.ExecContext(ctx, "DROP DATABASE `"+dbName+"`")
		_ = db.Close()
		t.Fatalf("mysql.Open: %v", err)
	}

	cleanup := func() {
		_ = st.Close()
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE `"+dbName+"`")
		_ = db.Close()
	}

	return st, db, dbName, cleanup
}
