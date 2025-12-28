//go:build !rabbitmq

package broker

import "errors"

func openRabbitMQ(Config) (Broker, error) {
	return nil, errors.New("broker: rabbitmq adapter is not built; rebuild with -tags=rabbitmq")
}
