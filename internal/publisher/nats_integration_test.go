//go:build integration && nats

package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	"github.com/nats-io/nats.go"
)

func TestPublisher_NATS(t *testing.T) {
	if os.Getenv("JUNO_TEST_DOCKER") == "" {
		t.Skip("set JUNO_TEST_DOCKER=1 to run broker integration tests")
	}

	natsURL := os.Getenv("JUNO_TEST_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:14222"
	}

	topic := fmt.Sprintf("junoscan.test.%d", time.Now().UnixNano())

	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync(topic)
	if err != nil {
		t.Fatalf("nats subscribe: %v", err)
	}
	_ = nc.Flush()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	br, err := broker.Open(ctx, broker.Config{Driver: "nats", URL: natsURL, Topic: topic})
	if err != nil {
		t.Fatalf("broker.Open: %v", err)
	}
	defer func() { _ = br.Close() }()

	st, err := rocksdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if err := st.UpsertWallet(ctx, "hot", "jview1test"); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	payload := json.RawMessage(`{"txid":"nats-test-txid"}`)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertEvent(ctx, store.Event{
			Kind:     "DepositEvent",
			WalletID: "hot",
			Height:   123,
			Payload:  payload,
		})
	}); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	pub, err := New(st, br, Config{PollInterval: 10 * time.Millisecond, BatchSize: 10})
	if err != nil {
		t.Fatalf("publisher.New: %v", err)
	}
	if err := pub.publishOnce(ctx); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("nats NextMsg: %v", err)
	}

	var env broker.Envelope
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Kind != "DepositEvent" {
		t.Fatalf("env.kind=%q want %q", env.Kind, "DepositEvent")
	}
	if env.Wallet != "hot" {
		t.Fatalf("env.wallet_id=%q want %q", env.Wallet, "hot")
	}
	if env.Height != 123 {
		t.Fatalf("env.height=%d want %d", env.Height, 123)
	}
	if string(env.Payload) != string(payload) {
		t.Fatalf("env.payload=%s want %s", string(env.Payload), string(payload))
	}
}
