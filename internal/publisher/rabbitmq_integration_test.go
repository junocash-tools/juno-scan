//go:build integration && rabbitmq

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
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisher_RabbitMQ(t *testing.T) {
	if os.Getenv("JUNO_TEST_DOCKER") == "" {
		t.Skip("set JUNO_TEST_DOCKER=1 to run broker integration tests")
	}

	rabbitURL := os.Getenv("JUNO_TEST_RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@127.0.0.1:25672/"
	}

	queue := fmt.Sprintf("junoscan.test.%d", time.Now().UnixNano())

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("amqp channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}

	msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	br, err := broker.Open(ctx, broker.Config{Driver: "rabbitmq", URL: rabbitURL, Topic: queue})
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

	payload := json.RawMessage(`{"txid":"rabbitmq-test-txid"}`)
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

	select {
	case d := <-msgs:
		var env broker.Envelope
		if err := json.Unmarshal(d.Body, &env); err != nil {
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
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for rabbitmq message")
	}
}
