//go:build !cgo

package ffi

import "errors"

func ScanTxJSON(string) (string, error) {
	return "", errors.New("scan: cgo disabled")
}
