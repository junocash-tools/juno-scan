package api

import (
	"context"
	"errors"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

type fakeWitnessFastStore struct {
	treeSize int64

	targetByPos map[int64]store.OrchardCommitment
	rootByIndex map[int64]store.OrchardSubtreeRoot

	rangeCalls [][2]int64
}

func (f *fakeWitnessFastStore) OrchardTreeSizeAtHeight(context.Context, int64) (int64, error) {
	return f.treeSize, nil
}

func (f *fakeWitnessFastStore) ListOrchardCommitmentCMXByPositionRange(_ context.Context, _ int64, startPos, endPos int64) ([]string, error) {
	f.rangeCalls = append(f.rangeCalls, [2]int64{startPos, endPos})
	if endPos < startPos {
		return nil, errors.New("bad range")
	}
	out := make([]string, int(endPos-startPos))
	for i := range out {
		out[i] = "cmx"
	}
	return out, nil
}

func (f *fakeWitnessFastStore) ListOrchardCommitmentsByPositionsUpToHeight(_ context.Context, _ int64, positions []int64) ([]store.OrchardCommitment, error) {
	out := make([]store.OrchardCommitment, 0, len(positions))
	for _, pos := range positions {
		c, ok := f.targetByPos[pos]
		if !ok {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (f *fakeWitnessFastStore) ListOrchardSubtreeRootsByIndexRange(_ context.Context, startIndex int64, limit int) ([]store.OrchardSubtreeRoot, error) {
	if limit <= 0 {
		return nil, nil
	}
	end := startIndex + int64(limit)
	out := make([]store.OrchardSubtreeRoot, 0, limit)
	for idx, root := range f.rootByIndex {
		if idx >= startIndex && idx < end {
			out = append(out, root)
		}
	}
	return out, nil
}

func TestComputeOrchardWitnessFast_UsesSubtreeRootsForNonTargetedSubtrees(t *testing.T) {
	oldFn := orchardWitnessWithOpsFn
	defer func() { orchardWitnessWithOpsFn = oldFn }()

	var gotTargets []orchardscan.WitnessTarget
	var gotOps []orchardscan.WitnessOperation
	orchardWitnessWithOpsFn = func(_ context.Context, _ uint32, targets []orchardscan.WitnessTarget, ops []orchardscan.WitnessOperation) (orchardscan.WitnessResult, error) {
		gotTargets = append([]orchardscan.WitnessTarget(nil), targets...)
		gotOps = append([]orchardscan.WitnessOperation(nil), ops...)
		return orchardscan.WitnessResult{Root: "root", Paths: []orchardscan.WitnessPath{{Position: 0, AuthPath: []string{"p"}}}}, nil
	}

	st := &fakeWitnessFastStore{
		treeSize: orchardSubtreeLeafCount * 2,
		targetByPos: map[int64]store.OrchardCommitment{
			0: {Position: 0, Height: 10, CMX: "target-cmx"},
		},
		rootByIndex: map[int64]store.OrchardSubtreeRoot{
			1: {SubtreeIndex: 1, Root: "subtree-root-1"},
		},
	}

	res, err := computeOrchardWitnessFast(context.Background(), st, 10, []uint32{0})
	if err != nil {
		t.Fatalf("computeOrchardWitnessFast: %v", err)
	}
	if res.Root != "root" {
		t.Fatalf("root=%q", res.Root)
	}
	if len(gotTargets) != 1 || gotTargets[0].CMXHex != "target-cmx" || gotTargets[0].Position != 0 {
		t.Fatalf("unexpected targets: %#v", gotTargets)
	}
	if len(st.rangeCalls) != 1 {
		t.Fatalf("range calls=%d want 1", len(st.rangeCalls))
	}
	if st.rangeCalls[0][0] != 0 || st.rangeCalls[0][1] != orchardSubtreeLeafCount {
		t.Fatalf("unexpected range call: %#v", st.rangeCalls[0])
	}

	var appendCount int
	var insertedRoots []string
	for _, op := range gotOps {
		switch op.Type {
		case orchardscan.WitnessOpAppendBatch:
			appendCount += len(op.CMXHex)
		case orchardscan.WitnessOpInsertSubtreeRoots:
			insertedRoots = append(insertedRoots, op.SubtreeRoots...)
		}
	}
	if appendCount != int(orchardSubtreeLeafCount) {
		t.Fatalf("append leaf count=%d want %d", appendCount, orchardSubtreeLeafCount)
	}
	if len(insertedRoots) != 1 || insertedRoots[0] != "subtree-root-1" {
		t.Fatalf("inserted roots=%#v", insertedRoots)
	}
}

func TestComputeOrchardWitnessFast_MissingTargetCommitmentIsInvalidRequest(t *testing.T) {
	st := &fakeWitnessFastStore{
		treeSize:    orchardSubtreeLeafCount,
		targetByPos: map[int64]store.OrchardCommitment{},
		rootByIndex: map[int64]store.OrchardSubtreeRoot{},
		rangeCalls:  nil,
	}

	_, err := computeOrchardWitnessFast(context.Background(), st, 10, []uint32{0})
	if err == nil {
		t.Fatalf("expected error")
	}
	var oe *orchardscan.Error
	if !errors.As(err, &oe) || oe.Code != orchardscan.ErrInvalidRequest {
		t.Fatalf("expected invalid request, got %v", err)
	}
}
