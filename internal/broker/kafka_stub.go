//go:build !kafka

package broker

import (
	"errors"
)

func openKafka(Config) (Broker, error) {
	return nil, errors.New("broker: kafka adapter is not built; rebuild with -tags=kafka")
}

