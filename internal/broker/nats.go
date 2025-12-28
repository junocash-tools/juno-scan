//go:build nats

package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type natsBroker struct {
	nc    *nats.Conn
	topic string
}

func openNATS(cfg Config) (Broker, error) {
	nc, err := nats.Connect(cfg.URL, nats.Name("juno-scan"), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("broker: nats connect: %w", err)
	}
	return &natsBroker{nc: nc, topic: cfg.Topic}, nil
}

func (b *natsBroker) Publish(ctx context.Context, key string, value []byte) error {
	_ = ctx

	msg := &nats.Msg{
		Subject: b.topic,
		Data:    value,
	}
	if key != "" {
		msg.Header = nats.Header{}
		msg.Header.Set("x-key", key)
	}
	if err := b.nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("broker: nats publish: %w", err)
	}
	return nil
}

func (b *natsBroker) Close() error {
	if b == nil || b.nc == nil {
		return nil
	}
	b.nc.Close()
	return nil
}
