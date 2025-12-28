package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type Broker interface {
	Publish(ctx context.Context, key string, value []byte) error
	Close() error
}

type Config struct {
	Driver string
	URL    string
	Topic  string
}

func Open(ctx context.Context, cfg Config) (Broker, error) {
	_ = ctx

	driver := strings.ToLower(strings.TrimSpace(cfg.Driver))
	switch driver {
	case "", "none":
		return nil, nil
	}

	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("broker: url is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("broker: topic is required")
	}

	switch driver {
	case "kafka":
		return openKafka(cfg)
	case "nats":
		return openNATS(cfg)
	case "rabbitmq":
		return openRabbitMQ(cfg)
	default:
		return nil, fmt.Errorf("broker: unsupported driver %q", cfg.Driver)
	}
}

