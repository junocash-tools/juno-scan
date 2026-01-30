//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func TestScanner_DepositDetected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	deposit := waitForEventKind(t, ctx, st, "hot", "DepositEvent")

	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	confirmed := waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	var confirmedPayload struct {
		TxID                  string `json:"txid"`
		RequiredConfirmations int64  `json:"required_confirmations"`
		ConfirmedHeight       int64  `json:"confirmed_height"`
		Status                struct {
			Confirmations int64 `json:"confirmations"`
		} `json:"status"`
	}
	if err := json.Unmarshal(confirmed.Payload, &confirmedPayload); err != nil {
		t.Fatalf("unmarshal confirmed payload: %v", err)
	}
	if confirmedPayload.TxID == "" {
		t.Fatalf("missing txid in confirmed payload")
	}
	if confirmedPayload.TxID != mustTxIDFromPayload(t, deposit.Payload) {
		t.Fatalf("confirmed txid mismatch")
	}
	if confirmedPayload.RequiredConfirmations != 2 {
		t.Fatalf("required_confirmations=%d want 2", confirmedPayload.RequiredConfirmations)
	}
	if confirmedPayload.Status.Confirmations != 2 {
		t.Fatalf("status.confirmations=%d want 2", confirmedPayload.Status.Confirmations)
	}
	if confirmedPayload.ConfirmedHeight <= 0 {
		t.Fatalf("invalid confirmed_height=%d", confirmedPayload.ConfirmedHeight)
	}

	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// Spend the detected note.
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
	spendConfirmed := waitForEventKind(t, ctx, st, "hot", "SpendConfirmed")
	outgoingConfirmed := waitForEventKind(t, ctx, st, "hot", "OutgoingOutputConfirmed")

	var spendConfirmedPayload struct {
		TxID                  string `json:"txid"`
		NoteTxID              string `json:"note_txid"`
		RequiredConfirmations int64  `json:"required_confirmations"`
		ConfirmedHeight       int64  `json:"confirmed_height"`
		Status                struct {
			Confirmations int64 `json:"confirmations"`
		} `json:"status"`
	}
	if err := json.Unmarshal(spendConfirmed.Payload, &spendConfirmedPayload); err != nil {
		t.Fatalf("unmarshal spend confirmed payload: %v", err)
	}
	if spendConfirmedPayload.TxID == "" {
		t.Fatalf("missing txid in spend confirmed payload")
	}
	if spendConfirmedPayload.NoteTxID != mustTxIDFromPayload(t, deposit.Payload) {
		t.Fatalf("spend confirmed note_txid mismatch")
	}
	if spendConfirmedPayload.RequiredConfirmations != 2 {
		t.Fatalf("required_confirmations=%d want 2", spendConfirmedPayload.RequiredConfirmations)
	}
	if spendConfirmedPayload.Status.Confirmations != 2 {
		t.Fatalf("status.confirmations=%d want 2", spendConfirmedPayload.Status.Confirmations)
	}
	if spendConfirmedPayload.ConfirmedHeight <= 0 {
		t.Fatalf("invalid confirmed_height=%d", spendConfirmedPayload.ConfirmedHeight)
	}

	var outgoingConfirmedPayload struct {
		TxID                  string `json:"txid"`
		RequiredConfirmations int64  `json:"required_confirmations"`
		ConfirmedHeight       int64  `json:"confirmed_height"`
	}
	if err := json.Unmarshal(outgoingConfirmed.Payload, &outgoingConfirmedPayload); err != nil {
		t.Fatalf("unmarshal outgoing confirmed payload: %v", err)
	}
	if outgoingConfirmedPayload.TxID != spendTxID {
		t.Fatalf("outgoing confirmed txid mismatch")
	}
	if outgoingConfirmedPayload.RequiredConfirmations != 2 {
		t.Fatalf("required_confirmations=%d want 2", outgoingConfirmedPayload.RequiredConfirmations)
	}
	if outgoingConfirmedPayload.ConfirmedHeight <= 0 {
		t.Fatalf("invalid confirmed_height=%d", outgoingConfirmedPayload.ConfirmedHeight)
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
		}
	}
	if !foundSpent {
		t.Fatalf("spent note not found")
	}
}

func TestScanner_DepositMemoExtracted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	sc, err := scanner.New(st, rpc, uaHRP, 100*time.Millisecond, 1, "")
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

	// Fund the wallet first (coinbase spends cannot make change).
	opidFund := mustShieldCoinbase(t, ctx, jd, fromAddr, addr)
	mustWaitOpSuccess(t, ctx, jd, opidFund)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))
	mustWaitOrchardBalanceForViewingKey(t, ctx, jd, ufvk, 2)

	// A memo of "00" (padded by the node) should be detectable (not the "no memo" marker 0xF6).
	opid := mustSendManyWithMemo(t, ctx, jd, addr, addr, "0.01", "00")
	mustWaitOpSuccess(t, ctx, jd, opid)
	mustRun(t, jd.CLICommand(ctx, "generate", "1"))

	deposit := waitForEventWithMemoPrefix(t, ctx, st, "hot", "DepositEvent", "00")

	var payload struct {
		MemoHex string `json:"memo_hex"`
	}
	if err := json.Unmarshal(deposit.Payload, &payload); err != nil {
		t.Fatalf("unmarshal deposit payload: %v", err)
	}
	if payload.MemoHex == "" {
		t.Fatalf("expected memo_hex in deposit payload")
	}
	if !strings.HasPrefix(payload.MemoHex, "00") {
		t.Fatalf("unexpected memo_hex prefix: %q", payload.MemoHex[:min(8, len(payload.MemoHex))])
	}

	confirmed := waitForEventWithMemoPrefix(t, ctx, st, "hot", "DepositConfirmed", payload.MemoHex)
	var confirmedPayload struct {
		MemoHex string `json:"memo_hex"`
	}
	if err := json.Unmarshal(confirmed.Payload, &confirmedPayload); err != nil {
		t.Fatalf("unmarshal confirmed payload: %v", err)
	}
	if confirmedPayload.MemoHex != payload.MemoHex {
		t.Fatalf("confirmed memo mismatch")
	}
}

func mustCreateWalletAndUFVK(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) (addr string, ufvk string) {
	t.Helper()

	var acc struct {
		Account int `json:"account"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getnewaccount"), &acc)

	var addrResp struct {
		Address string `json:"address"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getaddressforaccount", strconvI(acc.Account)), &addrResp)
	if addrResp.Address == "" {
		t.Fatalf("missing address")
	}

	out := mustRun(t, jd.CLICommand(ctx, "z_exportviewingkey", addrResp.Address))
	ufvk = strings.TrimSpace(string(out))
	if ufvk == "" {
		t.Fatalf("missing ufvk")
	}
	return addrResp.Address, ufvk
}

func mustCreateUnifiedAddress(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) (addr string) {
	t.Helper()

	var acc struct {
		Account int `json:"account"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getnewaccount"), &acc)

	var addrResp struct {
		Address string `json:"address"`
	}
	mustRunJSON(t, jd.CLICommand(ctx, "z_getaddressforaccount", strconvI(acc.Account)), &addrResp)
	if addrResp.Address == "" {
		t.Fatalf("missing address")
	}
	return addrResp.Address
}

func mustCoinbaseAddress(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd) string {
	t.Helper()

	var utxos []struct {
		Address string `json:"address"`
	}
	out := mustRun(t, jd.CLICommand(ctx, "listunspent", "1", "9999999"))
	if err := json.Unmarshal(out, &utxos); err != nil {
		t.Fatalf("listunspent json: %v\n%s", err, string(out))
	}
	if len(utxos) == 0 || utxos[0].Address == "" {
		t.Fatalf("no utxos found")
	}
	return utxos[0].Address
}

func mustShieldCoinbase(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr, toAddr string) string {
	t.Helper()

	out := mustRun(t, jd.CLICommand(ctx, "z_shieldcoinbase", fromAddr, toAddr))

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

func mustSendMany(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr, toAddr string, amount string) string {
	t.Helper()

	recipients := `[{"address":"` + toAddr + `","amount":` + amount + `}]`
	out := mustRun(t, jd.CLICommand(ctx, "z_sendmany", fromAddr, recipients, "1"))

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

func mustSendManyWithMemo(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, fromAddr, toAddr string, amount string, memoHex string) string {
	t.Helper()

	recipients := `[{"address":"` + toAddr + `","amount":` + amount + `,"memo":"` + memoHex + `"}]`
	out := mustRun(t, jd.CLICommand(ctx, "z_sendmany", fromAddr, recipients, "1"))

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

func mustWaitOpSuccess(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, opid string) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		out := mustRun(t, jd.CLICommand(ctx, "z_getoperationstatus", `["`+opid+`"]`))
		var res []struct {
			Status string `json:"status"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}
		if err := json.Unmarshal(out, &res); err == nil && len(res) > 0 {
			switch res[0].Status {
			case "success":
				return
			case "failed":
				msg := ""
				if res[0].Error != nil {
					msg = res[0].Error.Message
				}
				t.Fatalf("operation failed: %s (%s)", opid, msg)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("operation did not succeed: %s", opid)
}

func mustWaitOrchardBalanceForViewingKey(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, ufvk string, minconf int) int64 {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type pool struct {
		ValueZat int64 `json:"valueZat"`
	}
	type resp struct {
		Pools map[string]pool `json:"pools"`
	}

	for time.Now().Before(deadline) {
		out := mustRun(t, jd.CLICommand(ctx, "z_getbalanceforviewingkey", ufvk, strconvI(minconf)))
		var r resp
		if err := json.Unmarshal(out, &r); err == nil {
			if p, ok := r.Pools["orchard"]; ok && p.ValueZat > 0 {
				return p.ValueZat
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("orchard balance not available for minconf=%d", minconf)
	return 0
}

func mustTxIDForOpID(t *testing.T, ctx context.Context, jd *testutil.RunningJunocashd, opid string) string {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		out := mustRun(t, jd.CLICommand(ctx, "z_getoperationresult", `["`+opid+`"]`))
		var res []struct {
			Status string `json:"status"`
			Result *struct {
				TxID string `json:"txid"`
			} `json:"result,omitempty"`
			Error *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatalf("op result decode failed: %v", err)
		}
		if len(res) == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		switch res[0].Status {
		case "success":
			if res[0].Result == nil || strings.TrimSpace(res[0].Result.TxID) == "" {
				t.Fatalf("missing txid for opid %s", opid)
			}
			return strings.TrimSpace(res[0].Result.TxID)
		case "failed":
			msg := ""
			if res[0].Error != nil {
				msg = res[0].Error.Message
			}
			t.Fatalf("operation failed: %s (%s)", opid, msg)
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("txid not available for opid %s", opid)
	return ""
}

func waitForPendingSpend(t *testing.T, ctx context.Context, st store.Store, walletID string, spendTxID string) {
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
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("pending spend not detected for txid=%s", spendTxID)
}

func waitForEventKind(t *testing.T, ctx context.Context, st store.Store, walletID string, kind string) store.Event {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		events, _, err := st.ListWalletEvents(ctx, walletID, 0, 1000, store.EventFilter{})
		if err == nil {
			for _, e := range events {
				if e.Kind == kind {
					return e
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s not found", kind)
	return store.Event{}
}

func waitForEventWithMemoPrefix(t *testing.T, ctx context.Context, st store.Store, walletID string, kind string, memoPrefix string) store.Event {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID    string `json:"txid"`
		MemoHex string `json:"memo_hex"`
	}

	for time.Now().Before(deadline) {
		events, _, err := st.ListWalletEvents(ctx, walletID, 0, 1000, store.EventFilter{})
		if err == nil {
			for _, e := range events {
				if e.Kind != kind {
					continue
				}
				var p payload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					continue
				}
				if p.MemoHex == "" {
					continue
				}
				if memoPrefix == "" || strings.HasPrefix(p.MemoHex, memoPrefix) {
					return e
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("%s with memo_prefix=%q not found", kind, memoPrefix)
	return store.Event{}
}

func waitForOutgoingOutputEventState(t *testing.T, ctx context.Context, st store.Store, walletID string, txid string, wantState string) store.Event {
	t.Helper()

	txid = strings.ToLower(strings.TrimSpace(txid))

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	type payload struct {
		TxID            string `json:"txid"`
		Height          *int64 `json:"height,omitempty"`
		ActionIndex     uint32 `json:"action_index"`
		AmountZatoshis  uint64 `json:"amount_zatoshis"`
		RecipientAddress string `json:"recipient_address"`
		Status          struct {
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
				if p.Status.State != wantState {
					continue
				}
				if strings.TrimSpace(p.RecipientAddress) == "" {
					t.Fatalf("missing recipient_address in outgoing output payload")
				}
				if p.AmountZatoshis == 0 {
					t.Fatalf("missing amount_zatoshis in outgoing output payload")
				}
				if wantState == "mempool" && p.Height != nil {
					t.Fatalf("expected no payload.height for mempool outgoing output")
				}
				if wantState == "confirmed" && p.Height == nil {
					t.Fatalf("expected payload.height for confirmed outgoing output")
				}
				return e
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("OutgoingOutputEvent txid=%s state=%s not found", txid, wantState)
	return store.Event{}
}

func mustTxIDFromPayload(t *testing.T, payload json.RawMessage) string {
	t.Helper()

	var out struct {
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(payload, &out); err != nil || out.TxID == "" {
		t.Fatalf("missing txid in payload: %v", err)
	}
	return out.TxID
}

func mustRun(t *testing.T, cmd *exec.Cmd) []byte {
	t.Helper()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(cmd.Args, " "), err, string(out))
	}
	return out
}

func mustRunJSON(t *testing.T, cmd *exec.Cmd, out any) {
	t.Helper()
	b := mustRun(t, cmd)
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("%s: unmarshal: %v\n%s", strings.Join(cmd.Args, " "), err, string(b))
	}
}

func strconvI(n int) string {
	return strconv.FormatInt(int64(n), 10)
}
