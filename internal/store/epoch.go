package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

const EventEpochHexLength = 64

func NewEventEpoch() (string, error) {
	var raw [EventEpochHexLength / 2]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("event epoch: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func UFVKFingerprint(ufvk string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(ufvk)))
	return hex.EncodeToString(sum[:])
}
