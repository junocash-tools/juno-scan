package ffi

/*
#cgo CFLAGS: -I${SRCDIR}/../../rust/scan/include
#cgo LDFLAGS: -L${SRCDIR}/../../rust/scan/target/release -ljuno_scan

#include "juno_scan.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

var errNull = errors.New("scan: null response")

func ScanTxJSON(req string) (string, error) {
	cReq := C.CString(req)
	defer C.free(unsafe.Pointer(cReq))

	out := C.juno_scan_scan_tx_json(cReq)
	if out == nil {
		return "", errNull
	}
	defer C.juno_scan_string_free(out)

	return C.GoString(out), nil
}

func RecoverOutgoingTxJSON(req string) (string, error) {
	cReq := C.CString(req)
	defer C.free(unsafe.Pointer(cReq))

	out := C.juno_scan_recover_outgoing_tx_json(cReq)
	if out == nil {
		return "", errNull
	}
	defer C.juno_scan_string_free(out)

	return C.GoString(out), nil
}

func ValidateUFVKJSON(req string) (string, error) {
	cReq := C.CString(req)
	defer C.free(unsafe.Pointer(cReq))

	out := C.juno_scan_validate_ufvk_json(cReq)
	if out == nil {
		return "", errNull
	}
	defer C.juno_scan_string_free(out)

	return C.GoString(out), nil
}

func OrchardWitnessJSON(req string) (string, error) {
	cReq := C.CString(req)
	defer C.free(unsafe.Pointer(cReq))

	out := C.juno_scan_orchard_witness_json(cReq)
	if out == nil {
		return "", errNull
	}
	defer C.juno_scan_string_free(out)

	return C.GoString(out), nil
}
