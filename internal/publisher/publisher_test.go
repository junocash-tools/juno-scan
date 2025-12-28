package publisher

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

type fakeBroker struct {
	msgs []published
}

type published struct {
	key   string
	value []byte
}

func (b *fakeBroker) Publish(_ context.Context, key string, value []byte) error {
	b.msgs = append(b.msgs, published{key: key, value: append([]byte{}, value...)})
	return nil
}

func (b *fakeBroker) Close() error { return nil }

func TestPublisher_PublishesAndAdvancesCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	payload := json.RawMessage(`{"txid":"tx1","k":"v"}`)
	if err := st.WithTx(ctx, func(tx store.Tx) error {
		return tx.InsertEvent(ctx, store.Event{
			Kind:     "DepositEvent",
			WalletID: "hot",
			Height:   1,
			Payload:  payload,
		})
	}); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	br := &fakeBroker{}
	p, err := New(st, br, Config{PollInterval: 10 * time.Millisecond, BatchSize: 100})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := p.publishOnce(ctx); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	if len(br.msgs) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(br.msgs))
	}
	if br.msgs[0].key != "tx1" {
		t.Fatalf("expected key tx1, got %q", br.msgs[0].key)
	}

	var env broker.Envelope
	if err := json.Unmarshal(br.msgs[0].value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Kind != "DepositEvent" || env.Wallet != "hot" || env.Height != 1 {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if string(env.Payload) != string(payload) {
		t.Fatalf("unexpected payload: %s", string(env.Payload))
	}

	cursor, err := st.WalletEventPublishCursor(ctx, "hot")
	if err != nil {
		t.Fatalf("WalletEventPublishCursor: %v", err)
	}
	if cursor == 0 {
		t.Fatalf("expected cursor > 0")
	}

	if err := p.publishOnce(ctx); err != nil {
		t.Fatalf("publishOnce 2: %v", err)
	}
	if len(br.msgs) != 1 {
		t.Fatalf("expected no additional publishes, got %d", len(br.msgs))
	}
}

