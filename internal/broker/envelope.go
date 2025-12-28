package broker

import "encoding/json"

type Envelope struct {
	Version string          `json:"version"`
	Kind    string          `json:"kind"`
	Wallet  string          `json:"wallet_id"`
	Height  int64           `json:"height"`
	Payload json.RawMessage `json:"payload"`
}

