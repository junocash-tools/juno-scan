//go:build kafka

package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type kafkaBroker struct {
	w *kafka.Writer
}

func openKafka(cfg Config) (Broker, error) {
	brokers := splitCommaList(cfg.URL)
	if len(brokers) == 0 {
		return nil, errors.New("broker: kafka url must be a comma-separated list of brokers")
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 50 * time.Millisecond,
	}
	return &kafkaBroker{w: w}, nil
}

func (b *kafkaBroker) Publish(ctx context.Context, key string, value []byte) error {
	var keyBytes []byte
	if key != "" {
		keyBytes = []byte(key)
	}
	if err := b.w.WriteMessages(ctx, kafka.Message{
		Key:   keyBytes,
		Value: value,
	}); err != nil {
		return fmt.Errorf("broker: kafka publish: %w", err)
	}
	return nil
}

func (b *kafkaBroker) Close() error {
	if b == nil || b.w == nil {
		return nil
	}
	return b.w.Close()
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
