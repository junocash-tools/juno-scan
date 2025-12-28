package orchardscan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/Abdullah1738/juno-scan/internal/ffi"
)

type Wallet struct {
	WalletID string
	UFVK     string
}

type Action struct {
	ActionIndex     uint32
	ActionNullifier string
	CMX             string
	EphemeralKey    string
	EncCiphertext   string
}

type Note struct {
	WalletID         string
	ActionIndex      uint32
	DiversifierIndex uint32
	RecipientAddress string
	ValueZat         uint64
	NoteNullifier    string
}

type Result struct {
	Actions []Action
	Notes   []Note
}

type ErrorCode string

const (
	ErrReqJSONInvalid             ErrorCode = "req_json_invalid"
	ErrTxHexInvalid               ErrorCode = "tx_hex_invalid"
	ErrTxParseFailed              ErrorCode = "tx_parse_failed"
	ErrUFVKInvalid                ErrorCode = "ufvk_invalid"
	ErrUFVKMissingOrchardReceiver ErrorCode = "ufvk_missing_orchard_receiver"
	ErrUFVKOrchardFVKLenInvalid   ErrorCode = "ufvk_orchard_fvk_len_invalid"
	ErrUFVKOrchardFVKBytesInvalid ErrorCode = "ufvk_orchard_fvk_bytes_invalid"
	ErrUAHrpInvalid               ErrorCode = "ua_hrp_invalid"
	ErrInvalidRequest             ErrorCode = "invalid_request"
	ErrInternal                   ErrorCode = "internal"
	ErrPanic                      ErrorCode = "panic"
)

type Error struct {
	Code ErrorCode
}

func (e *Error) Error() string {
	return fmt.Sprintf("orchardscan: %s", e.Code)
}

func ScanTx(ctx context.Context, uaHRP string, wallets []Wallet, txHex string) (Result, error) {
	_ = ctx // reserved for future (ffi call is synchronous)

	req := scanTxRequest{
		UAHRP:   uaHRP,
		Wallets: make([]walletIn, 0, len(wallets)),
		TxHex:   txHex,
	}
	for _, w := range wallets {
		req.Wallets = append(req.Wallets, walletIn{
			WalletID: w.WalletID,
			UFVK:     w.UFVK,
		})
	}

	b, err := json.Marshal(req)
	if err != nil {
		return Result{}, errors.New("orchardscan: marshal request")
	}

	raw, err := ffi.ScanTxJSON(string(b))
	if err != nil {
		return Result{}, err
	}

	var resp scanTxResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return Result{}, errors.New("orchardscan: invalid response")
	}

	switch resp.Status {
	case "ok":
		out := Result{
			Actions: make([]Action, 0, len(resp.Actions)),
			Notes:   make([]Note, 0, len(resp.Notes)),
		}
		for _, a := range resp.Actions {
			out.Actions = append(out.Actions, Action{
				ActionIndex:     a.ActionIndex,
				ActionNullifier: a.ActionNullifier,
				CMX:             a.CMX,
				EphemeralKey:    a.EphemeralKey,
				EncCiphertext:   a.EncCiphertext,
			})
		}
		for _, n := range resp.Notes {
			v, err := strconv.ParseUint(n.ValueZat, 10, 64)
			if err != nil {
				return Result{}, errors.New("orchardscan: invalid response")
			}
			out.Notes = append(out.Notes, Note{
				WalletID:         n.WalletID,
				ActionIndex:      n.ActionIndex,
				DiversifierIndex: n.DiversifierIndex,
				RecipientAddress: n.RecipientAddress,
				ValueZat:         v,
				NoteNullifier:    n.NoteNullifier,
			})
		}
		return out, nil
	case "err":
		if resp.Error == "" {
			return Result{}, errors.New("orchardscan: invalid response")
		}
		return Result{}, &Error{Code: ErrorCode(resp.Error)}
	default:
		return Result{}, errors.New("orchardscan: invalid response")
	}
}

type scanTxRequest struct {
	UAHRP   string     `json:"ua_hrp"`
	Wallets []walletIn `json:"wallets"`
	TxHex   string     `json:"tx_hex"`
}

type walletIn struct {
	WalletID string `json:"wallet_id"`
	UFVK     string `json:"ufvk"`
}

type scanTxResponse struct {
	Status  string      `json:"status"`
	Actions []actionOut `json:"actions,omitempty"`
	Notes   []noteOut   `json:"notes,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type actionOut struct {
	ActionIndex     uint32 `json:"action_index"`
	ActionNullifier string `json:"action_nullifier"`
	CMX             string `json:"cmx"`
	EphemeralKey    string `json:"ephemeral_key"`
	EncCiphertext   string `json:"enc_ciphertext"`
}

type noteOut struct {
	WalletID         string `json:"wallet_id"`
	ActionIndex      uint32 `json:"action_index"`
	DiversifierIndex uint32 `json:"diversifier_index"`
	RecipientAddress string `json:"recipient_address"`
	ValueZat         string `json:"value_zat"`
	NoteNullifier    string `json:"note_nullifier"`
}
