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
	return callOrchardWitness(req)
}

type WitnessTarget struct {
	Position uint32
	CMXHex   string
}

type WitnessOperationType string

const (
	WitnessOpAppendBatch        WitnessOperationType = "append_batch"
	WitnessOpInsertSubtreeRoots WitnessOperationType = "insert_subtree_roots"
)

type WitnessOperation struct {
	Type         WitnessOperationType
	CMXHex       []string
	SubtreeRoots []string
}

func OrchardWitnessWithOps(ctx context.Context, anchorHeight uint32, targets []WitnessTarget, ops []WitnessOperation) (WitnessResult, error) {
	_ = ctx // reserved for future (ffi call is synchronous)

	req := witnessRequest{
		AnchorHeight: &anchorHeight,
		Targets:      make([]witnessTargetReq, 0, len(targets)),
		Ops:          make([]witnessOperationReq, 0, len(ops)),
	}
	for _, t := range targets {
		req.Targets = append(req.Targets, witnessTargetReq{
			Position: t.Position,
			CMXHex:   t.CMXHex,
		})
	}
	for _, op := range ops {
		req.Ops = append(req.Ops, witnessOperationReq{
			Type:         string(op.Type),
			CMXHex:       op.CMXHex,
			SubtreeRoots: op.SubtreeRoots,
		})
	}
	return callOrchardWitness(req)
}

func OrchardSubtreeRoot(ctx context.Context, cmxHex []string) (string, error) {
	_ = ctx // reserved for future (ffi call is synchronous)

	b, err := json.Marshal(subtreeRootRequest{CMXHex: cmxHex})
	if err != nil {
		return "", errors.New("orchardscan: marshal request")
	}

	raw, err := ffi.OrchardSubtreeRootJSON(string(b))
	if err != nil {
		return "", err
	}

	var resp subtreeRootResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return "", errors.New("orchardscan: invalid response")
	}

	switch resp.Status {
	case "ok":
		if resp.Root == "" {
			return "", errors.New("orchardscan: invalid response")
		}
		return resp.Root, nil
	case "err":
		if resp.Error == "" {
			return "", errors.New("orchardscan: invalid response")
		}
		return "", &Error{Code: ErrorCode(resp.Error)}
	default:
		return "", errors.New("orchardscan: invalid response")
	}
}

func callOrchardWitness(req witnessRequest) (WitnessResult, error) {
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
	CMXHex       []string              `json:"cmx_hex,omitempty"`
	Positions    []uint32              `json:"positions,omitempty"`
	AnchorHeight *uint32               `json:"anchor_height,omitempty"`
	Targets      []witnessTargetReq    `json:"targets,omitempty"`
	Ops          []witnessOperationReq `json:"ops,omitempty"`
}

type witnessTargetReq struct {
	Position uint32 `json:"position"`
	CMXHex   string `json:"cmx_hex"`
}

type witnessOperationReq struct {
	Type         string   `json:"type"`
	CMXHex       []string `json:"cmx_hex,omitempty"`
	SubtreeRoots []string `json:"subtree_roots,omitempty"`
}

type subtreeRootRequest struct {
	CMXHex []string `json:"cmx_hex"`
}

type witnessResponse struct {
	Status string            `json:"status"`
	Root   string            `json:"root,omitempty"`
	Paths  []witnessPathResp `json:"paths,omitempty"`
	Error  string            `json:"error,omitempty"`
}

type subtreeRootResponse struct {
	Status string `json:"status"`
	Root   string `json:"root,omitempty"`
	Error  string `json:"error,omitempty"`
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
