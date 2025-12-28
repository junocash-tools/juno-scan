//go:build integration && kafka

package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
	kafka "github.com/segmentio/kafka-go"
)

func TestPublisher_Kafka(t *testing.T) {
	if os.Getenv("JUNO_TEST_DOCKER") == "" {
		t.Skip("set JUNO_TEST_DOCKER=1 to run broker integration tests")
	}

	kafkaBrokers := os.Getenv("JUNO_TEST_KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "127.0.0.1:19092"
	}
	brokers := splitCommaList(kafkaBrokers)
	if len(brokers) == 0 {
		t.Fatalf("invalid kafka brokers: %q", kafkaBrokers)
	}

	topic := fmt.Sprintf("junoscan.test.%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	br, err := broker.Open(ctx, broker.Config{Driver: "kafka", URL: kafkaBrokers, Topic: topic})
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

	payload := json.RawMessage(`{"txid":"kafka-test-txid"}`)
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1e6,
	})
	defer func() { _ = reader.Close() }()

	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("kafka ReadMessage: %v", err)
	}

	var env broker.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
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

func splitCommaList(s string) []string {
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		out = append(out, r)
	}
	return out
}
