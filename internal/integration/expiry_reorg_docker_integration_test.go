//go:build integration && docker

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/Abdullah1738/juno-scan/internal/testutil/containers"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/testcontainers/testcontainers-go"
)

func TestScanner_MempoolSpendExpires_AcrossSync_RocksDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	networkName := fmt.Sprintf("junoscan-expiry-%d", time.Now().UnixNano())
	net, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:   networkName,
			Driver: "bridge",
		},
	})
	if err != nil {
		t.Fatalf("start network: %v", err)
	}
	defer func() { _ = net.Remove(context.Background()) }()

	a, err := containers.StartJunocashdNode(ctx, containers.JunocashdNodeConfig{
		Network: networkName,
		Alias:   "nodea",
		P2PPort: 8233,
		ExtraArgs: []string{
			"-txexpirydelta=4",
		},
	})
	if err != nil {
		t.Fatalf("start node A: %v", err)
	}
	defer func() { _ = a.Terminate(context.Background()) }()

	b, err := containers.StartJunocashdNode(ctx, containers.JunocashdNodeConfig{
		Network: networkName,
		Alias:   "nodeb",
		P2PPort: 8233,
	})
	if err != nil {
		t.Fatalf("start node B: %v", err)
	}
	defer func() { _ = b.Terminate(context.Background()) }()

	bAddr, err := b.P2PAddress(ctx)
	if err != nil {
		t.Fatalf("B p2p address: %v", err)
	}

	// Connect A <-> B so they share the same chain up to the spend.
	mustRunNode(t, ctx, a, "addnode", bAddr, "onetry")
	waitForPeers(t, ctx, a, 1)

	// Setup scan store + scanner against node A.
	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	addr, ufvk := mustCreateWalletAndUFVKNode(t, ctx, a)
	uaHRP := strings.SplitN(addr, "1", 2)[0]

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := st.UpsertWallet(ctx, "hot", ufvk); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	rpc := sdkjunocashd.New(a.RPCURL, a.RPCUser, a.RPCPassword)
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

	// Fund on the shared chain (blocks mined on A are synced to B).
	mustRunNode(t, ctx, a, "generate", "101")
	waitForBlockCountAtLeast(t, ctx, b, 101)

	fromAddr := mustCoinbaseAddressNode(t, ctx, a)
	opid := mustShieldCoinbaseNode(t, ctx, a, fromAddr, addr)
	mustWaitOpSuccessNode(t, ctx, a, opid)
	mustRunNode(t, ctx, a, "generate", "1")
	waitForBlockCountAtLeast(t, ctx, b, 102)

	waitForEventKind(t, ctx, st, "hot", "DepositEvent")
	mustRunNode(t, ctx, a, "generate", "1")
	waitForBlockCountAtLeast(t, ctx, b, 103)
	waitForEventKind(t, ctx, st, "hot", "DepositConfirmed")

	mustWaitOrchardBalanceForViewingKeyNode(t, ctx, a, ufvk, 2)

	// Disconnect A and B so the spend tx only exists on A.
	disconnectAllPeers(t, ctx, a)
	waitForPeers(t, ctx, a, 0)

	to1 := mustCreateUnifiedAddressNode(t, ctx, a)
	to2 := mustCreateUnifiedAddressNode(t, ctx, a)
	opid2 := mustSendManyWithFeeNode(t, ctx, a, addr, []string{to1, to2}, []string{"0.01", "0.01"}, "1", "0.0001")
	mustWaitOpSuccessNode(t, ctx, a, opid2)
	spendTxID := mustTxIDForOpIDNode(t, ctx, a, opid2)

	rawHex := strings.TrimSpace(string(mustRunNode(t, ctx, a, "getrawtransaction", spendTxID)))
	if rawHex == "" {
		t.Fatalf("missing raw tx hex")
	}

	expiryHeight := waitForPendingSpendWithExpiryHeight(t, ctx, st, "hot", spendTxID)
	waitForOutgoingOutputEventMempoolWithExpiryHeight(t, ctx, st, "hot", spendTxID, expiryHeight)

	// Mine on B until chainHeight > expiry_height.
	tipB := mustBlockCountNode(t, ctx, b)
	blocksToMine := (expiryHeight + 1) - tipB
	if blocksToMine < 1 {
		blocksToMine = 1
	}
	mustRunNode(t, ctx, b, "generate", strconv.FormatInt(blocksToMine, 10))
	wantHeight := tipB + blocksToMine
	waitForBlockCountAtLeast(t, ctx, b, wantHeight)

	// Reconnect and let A sync up to B's height.
	mustRunNode(t, ctx, a, "addnode", bAddr, "onetry")
	waitForPeers(t, ctx, a, 1)
	waitForBlockCountAtLeast(t, ctx, a, wantHeight)

	// The tx should be dropped from mempool on A once it is expired.
	waitForTxNotInMempool(t, ctx, a, spendTxID)
	if _, err := a.ExecCLI(ctx, "sendrawtransaction", rawHex); err == nil {
		t.Fatalf("expected sendrawtransaction to fail for expired tx")
	}

	waitForOutgoingOutputExpiredEvent(t, ctx, st, "hot", spendTxID, expiryHeight)
	waitForPendingSpendCleared(t, ctx, st, "hot", spendTxID)
}

func mustRunNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, args ...string) []byte {
	t.Helper()
	out, err := n.ExecCLI(ctx, args...)
	if err != nil {
		t.Fatalf("junocash-cli %s: %v", strings.Join(args, " "), err)
	}
	return out
}

func mustRunNodeJSON(t *testing.T, ctx context.Context, n *containers.JunocashdNode, out any, args ...string) {
	t.Helper()
	b := mustRunNode(t, ctx, n, args...)
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("junocash-cli %s: unmarshal: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(b)))
	}
}

func mustBlockCountNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode) int64 {
	t.Helper()
	out := strings.TrimSpace(string(mustRunNode(t, ctx, n, "getblockcount")))
	h, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		t.Fatalf("getblockcount parse: %v (%q)", err, out)
	}
	return h
}

func waitForBlockCountAtLeast(t *testing.T, ctx context.Context, n *containers.JunocashdNode, want int64) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	for time.Now().Before(deadline) {
		if got := mustBlockCountNode(t, ctx, n); got >= want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for getblockcount >= %d", want)
}

func waitForPeers(t *testing.T, ctx context.Context, n *containers.JunocashdNode, want int) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		var peers []struct{}
		mustRunNodeJSON(t, ctx, n, &peers, "getpeerinfo")
		if len(peers) == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for peers=%d", want)
}

func disconnectAllPeers(t *testing.T, ctx context.Context, n *containers.JunocashdNode) {
	t.Helper()

	var peers []struct {
		ID   int64  `json:"id"`
		Addr string `json:"addr"`
	}
	mustRunNodeJSON(t, ctx, n, &peers, "getpeerinfo")
	for _, p := range peers {
		if strings.TrimSpace(p.Addr) != "" {
			if _, err := n.ExecCLI(ctx, "disconnectnode", p.Addr); err == nil {
				continue
			}
		}
		if p.ID > 0 {
			_, _ = n.ExecCLI(ctx, "disconnectnode", "", strconv.FormatInt(p.ID, 10))
		}
	}
}

func waitForTxNotInMempool(t *testing.T, ctx context.Context, n *containers.JunocashdNode, txid string) {
	t.Helper()

	txid = strings.ToLower(strings.TrimSpace(txid))

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		var txids []string
		mustRunNodeJSON(t, ctx, n, &txids, "getrawmempool")
		found := false
		for _, id := range txids {
			if strings.ToLower(strings.TrimSpace(id)) == txid {
				found = true
				break
			}
		}
		if !found {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("txid still in mempool: %s", txid)
}

func mustCreateWalletAndUFVKNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode) (addr string, ufvk string) {
	t.Helper()

	var acc struct {
		Account int `json:"account"`
	}
	mustRunNodeJSON(t, ctx, n, &acc, "z_getnewaccount")

	var addrResp struct {
		Address string `json:"address"`
	}
	mustRunNodeJSON(t, ctx, n, &addrResp, "z_getaddressforaccount", strconv.FormatInt(int64(acc.Account), 10))
	if strings.TrimSpace(addrResp.Address) == "" {
		t.Fatalf("missing address")
	}

	out := strings.TrimSpace(string(mustRunNode(t, ctx, n, "z_exportviewingkey", addrResp.Address)))
	if out == "" {
		t.Fatalf("missing ufvk")
	}

	return addrResp.Address, out
}

func mustCreateUnifiedAddressNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode) (addr string) {
	t.Helper()

	var acc struct {
		Account int `json:"account"`
	}
	mustRunNodeJSON(t, ctx, n, &acc, "z_getnewaccount")

	var addrResp struct {
		Address string `json:"address"`
	}
	mustRunNodeJSON(t, ctx, n, &addrResp, "z_getaddressforaccount", strconv.FormatInt(int64(acc.Account), 10))
	if strings.TrimSpace(addrResp.Address) == "" {
		t.Fatalf("missing address")
	}
	return addrResp.Address
}

func mustCoinbaseAddressNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode) string {
	t.Helper()

	var utxos []struct {
		Address string `json:"address"`
	}
	mustRunNodeJSON(t, ctx, n, &utxos, "listunspent", "1", "9999999")
	if len(utxos) == 0 || strings.TrimSpace(utxos[0].Address) == "" {
		t.Fatalf("no utxos found")
	}
	return strings.TrimSpace(utxos[0].Address)
}

func mustShieldCoinbaseNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, fromAddr, toAddr string) string {
	t.Helper()

	out := mustRunNode(t, ctx, n, "z_shieldcoinbase", fromAddr, toAddr)

	var resp struct {
		OpID string `json:"opid"`
	}
	if err := json.Unmarshal(out, &resp); err == nil && strings.TrimSpace(resp.OpID) != "" {
		return strings.TrimSpace(resp.OpID)
	}
	opid := strings.TrimSpace(string(out))
	if opid == "" {
		t.Fatalf("missing opid")
	}
	return opid
}

func mustSendManyWithFeeNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, fromAddr string, toAddrs []string, amounts []string, minconf string, fee string) string {
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

	out := mustRunNode(t, ctx, n, "z_sendmany", fromAddr, b.String(), strings.TrimSpace(minconf), strings.TrimSpace(fee))

	var resp struct {
		OpID string `json:"opid"`
	}
	if err := json.Unmarshal(out, &resp); err == nil && strings.TrimSpace(resp.OpID) != "" {
		return strings.TrimSpace(resp.OpID)
	}
	opid := strings.TrimSpace(string(out))
	if opid == "" {
		t.Fatalf("missing opid")
	}
	return opid
}

func mustWaitOpSuccessNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, opid string) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		var res []struct {
			Status string `json:"status"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}
		mustRunNodeJSON(t, ctx, n, &res, "z_getoperationstatus", `["`+opid+`"]`)
		if len(res) > 0 {
			switch strings.TrimSpace(res[0].Status) {
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

func mustTxIDForOpIDNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, opid string) string {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for time.Now().Before(deadline) {
		var res []struct {
			Status string `json:"status"`
			Result *struct {
				TxID string `json:"txid"`
			} `json:"result,omitempty"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error,omitempty"`
		}
		mustRunNodeJSON(t, ctx, n, &res, "z_getoperationresult", `["`+opid+`"]`)
		if len(res) > 0 {
			switch strings.TrimSpace(res[0].Status) {
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
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("txid not available for opid %s", opid)
	return ""
}

func mustWaitOrchardBalanceForViewingKeyNode(t *testing.T, ctx context.Context, n *containers.JunocashdNode, ufvk string, minconf int) int64 {
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
		var r resp
		mustRunNodeJSON(t, ctx, n, &r, "z_getbalanceforviewingkey", ufvk, strconv.FormatInt(int64(minconf), 10))
		if p, ok := r.Pools["orchard"]; ok && p.ValueZat > 0 {
			return p.ValueZat
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("orchard balance not available for minconf=%d", minconf)
	return 0
}
