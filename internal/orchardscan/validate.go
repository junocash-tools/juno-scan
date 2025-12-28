package orchardscan

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Abdullah1738/juno-scan/internal/ffi"
)

func ValidateUFVK(ctx context.Context, ufvk string) error {
	_ = ctx // reserved for future (ffi call is synchronous)

	req := map[string]any{
		"ufvk": ufvk,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return errors.New("orchardscan: marshal request")
	}

	raw, err := ffi.ValidateUFVKJSON(string(b))
	if err != nil {
		return err
	}

	var resp struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return errors.New("orchardscan: invalid response")
	}

	switch resp.Status {
	case "ok":
		return nil
	case "err":
		if resp.Error == "" {
			return errors.New("orchardscan: invalid response")
		}
		return &Error{Code: ErrorCode(resp.Error)}
	default:
		return errors.New("orchardscan: invalid response")
	}
}
