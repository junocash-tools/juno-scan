//go:build !nats

package broker

import "errors"

func openNATS(Config) (Broker, error) {
	return nil, errors.New("broker: nats adapter is not built; rebuild with -tags=nats")
}
