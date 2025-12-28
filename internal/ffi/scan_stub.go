//go:build !cgo

package ffi

import "errors"

func ScanTxJSON(string) (string, error) {
	return "", errors.New("scan: cgo disabled")
}

func ValidateUFVKJSON(string) (string, error) {
	return "", errors.New("scan: cgo disabled")
}

func OrchardWitnessJSON(string) (string, error) {
	return "", errors.New("scan: cgo disabled")
}
