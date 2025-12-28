package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

type Config struct {
	PollInterval time.Duration
	BatchSize    int
}

type Publisher struct {
	st store.Store
	br broker.Broker

	pollInterval time.Duration
	batchSize    int
}

func New(st store.Store, br broker.Broker, cfg Config) (*Publisher, error) {
	if st == nil {
		return nil, errors.New("publisher: store is nil")
	}
	if br == nil {
		return nil, errors.New("publisher: broker is nil")
	}

	poll := cfg.PollInterval
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 || batchSize > 5000 {
		batchSize = 1000
	}

	return &Publisher{
		st:           st,
		br:           br,
		pollInterval: poll,
		batchSize:    batchSize,
	}, nil
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		if err := p.publishOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publishOnce(ctx context.Context) error {
	wallets, err := p.st.ListWallets(ctx)
	if err != nil {
		return fmt.Errorf("publisher: list wallets: %w", err)
	}

	for _, w := range wallets {
		cursor, err := p.st.WalletEventPublishCursor(ctx, w.WalletID)
		if err != nil {
			return err
		}

		for {
			events, nextCursor, err := p.st.ListWalletEvents(ctx, w.WalletID, cursor, p.batchSize)
			if err != nil {
				return err
			}
			if len(events) == 0 {
				break
			}

			for _, e := range events {
				env := broker.Envelope{
					Version: "v1",
					Kind:    e.Kind,
					Wallet:  w.WalletID,
					Height:  e.Height,
					Payload: e.Payload,
				}
				value, err := json.Marshal(env)
				if err != nil {
					return fmt.Errorf("publisher: marshal envelope: %w", err)
				}

				key := eventKey(w.WalletID, e.Payload)
				if err := p.br.Publish(ctx, key, value); err != nil {
					return err
				}

				cursor = e.ID
				if err := p.st.SetWalletEventPublishCursor(ctx, w.WalletID, cursor); err != nil {
					return err
				}
			}

			cursor = nextCursor
		}
	}

	return nil
}

func eventKey(walletID string, payload json.RawMessage) string {
	var tx struct {
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(payload, &tx); err == nil && tx.TxID != "" {
		return tx.TxID
	}
	return walletID
}
