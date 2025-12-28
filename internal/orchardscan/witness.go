package orchardscan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Abdullah1738/juno-scan/internal/ffi"
)

type WitnessPath struct {
	Position uint32   `json:"position"`
	AuthPath []string `json:"auth_path"`
}

type WitnessResult struct {
	Root  string
	Paths []WitnessPath
}

func OrchardWitness(ctx context.Context, cmxHex []string, positions []uint32) (WitnessResult, error) {
	_ = ctx // reserved for future (ffi call is synchronous)

	req := witnessRequest{
		CMXHex:    cmxHex,
		Positions: positions,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return WitnessResult{}, errors.New("orchardscan: marshal request")
	}

	raw, err := ffi.OrchardWitnessJSON(string(b))
	if err != nil {
		return WitnessResult{}, err
	}

	var resp witnessResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return WitnessResult{}, errors.New("orchardscan: invalid response")
	}

	switch resp.Status {
	case "ok":
		out := WitnessResult{
			Root:  resp.Root,
			Paths: make([]WitnessPath, 0, len(resp.Paths)),
		}
		if out.Root == "" {
			return WitnessResult{}, errors.New("orchardscan: invalid response")
		}
		for _, p := range resp.Paths {
			out.Paths = append(out.Paths, WitnessPath{
				Position: p.Position,
				AuthPath: p.AuthPath,
			})
		}
		return out, nil
	case "err":
		if resp.Error == "" {
			return WitnessResult{}, errors.New("orchardscan: invalid response")
		}
		return WitnessResult{}, &Error{Code: ErrorCode(resp.Error)}
	default:
		return WitnessResult{}, errors.New("orchardscan: invalid response")
	}
}

type witnessRequest struct {
	CMXHex    []string `json:"cmx_hex"`
	Positions []uint32 `json:"positions"`
}

type witnessResponse struct {
	Status string            `json:"status"`
	Root   string            `json:"root,omitempty"`
	Paths  []witnessPathResp `json:"paths,omitempty"`
	Error  string            `json:"error,omitempty"`
}

type witnessPathResp struct {
	Position uint32   `json:"position"`
	AuthPath []string `json:"auth_path"`
}

func (r WitnessResult) PathForPosition(position uint32) ([]string, error) {
	for _, p := range r.Paths {
		if p.Position == position {
			return p.AuthPath, nil
		}
	}
	return nil, fmt.Errorf("orchardscan: witness not found for position %d", position)
}
