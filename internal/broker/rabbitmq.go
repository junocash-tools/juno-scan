//go:build rabbitmq

package broker

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMQBroker struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func openRabbitMQ(cfg Config) (Broker, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("broker: rabbitmq dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("broker: rabbitmq channel: %w", err)
	}

	if _, err := ch.QueueDeclare(
		cfg.Topic,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("broker: rabbitmq queue declare: %w", err)
	}

	return &rabbitMQBroker{conn: conn, ch: ch, queue: cfg.Topic}, nil
}

func (b *rabbitMQBroker) Publish(ctx context.Context, key string, value []byte) error {
	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         value,
	}
	if key != "" {
		pub.MessageId = key
	}
	if err := b.ch.PublishWithContext(ctx, "", b.queue, false, false, pub); err != nil {
		return fmt.Errorf("broker: rabbitmq publish: %w", err)
	}
	return nil
}

func (b *rabbitMQBroker) Close() error {
	if b == nil {
		return nil
	}
	if b.ch != nil {
		_ = b.ch.Close()
	}
	if b.conn != nil {
		_ = b.conn.Close()
	}
	return nil
}
