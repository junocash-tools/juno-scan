package api

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

type fakeWitnessFastStore struct {
	treeSize int64
	mu       sync.Mutex
	anchor   string

	targetByPos  map[int64]store.OrchardCommitment
	rootByIndex  map[int64]store.OrchardSubtreeRoot
	shardByIndex map[int64]store.OrchardShardRoot

	rangeCalls [][2]int64
}

func (f *fakeWitnessFastStore) HashAtHeight(_ context.Context, _ int64) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.anchor == "" {
		return "canonical", true, nil
	}
	return f.anchor, true, nil
}

func (f *fakeWitnessFastStore) OrchardTreeSizeAtHeight(context.Context, int64) (int64, error) {
	return f.treeSize, nil
}

func (f *fakeWitnessFastStore) ListOrchardShardRootsByIndexRange(_ context.Context, startIndex int64, limit int) ([]store.OrchardShardRoot, error) {
	end := startIndex + int64(limit)
	var out []store.OrchardShardRoot
	for idx, root := range f.shardByIndex {
		if idx >= startIndex && idx < end {
			out = append(out, root)
		}
	}
	return out, nil
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
			1: {SubtreeIndex: 1, EndPosition: 2*orchardSubtreeLeafCount - 1, EndHeight: 10, EndBlockHash: "canonical", Root: "subtree-root-1"},
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

func TestComputeOrchardWitnessCached_UsesShardRoots(t *testing.T) {
	oldFn := orchardWitnessWithOpsFn
	defer func() { orchardWitnessWithOpsFn = oldFn }()
	var gotOps []orchardscan.WitnessOperation
	orchardWitnessWithOpsFn = func(_ context.Context, _ uint32, _ []orchardscan.WitnessTarget, ops []orchardscan.WitnessOperation) (orchardscan.WitnessResult, error) {
		gotOps = append([]orchardscan.WitnessOperation(nil), ops...)
		return orchardscan.WitnessResult{Root: "root", Paths: []orchardscan.WitnessPath{{Position: 0}}}, nil
	}
	st := &fakeWitnessFastStore{
		treeSize:    2 * orchardShardLeafCount,
		targetByPos: map[int64]store.OrchardCommitment{0: {Position: 0, Height: 10, CMX: "target"}},
		rootByIndex: map[int64]store.OrchardSubtreeRoot{},
		shardByIndex: map[int64]store.OrchardShardRoot{
			1: {Version: 1, ShardIndex: 1, EndPosition: 2*orchardShardLeafCount - 1, EndHeight: 10, EndBlockHash: "canonical", Root: "shard-root-1"},
		},
	}
	if _, err := computeOrchardWitnessCached(context.Background(), st, 10, []uint32{0}, true); err != nil {
		t.Fatal(err)
	}
	var appended int
	var shardRoots []string
	for _, op := range gotOps {
		appended += len(op.CMXHex)
		shardRoots = append(shardRoots, op.ShardRoots...)
	}
	if appended != int(orchardShardLeafCount) || len(shardRoots) != 1 || shardRoots[0] != "shard-root-1" {
		t.Fatalf("appended=%d shard_roots=%v", appended, shardRoots)
	}
}

func TestComputeOrchardWitnessCached_RejectsRootsOutsideExactCoverage(t *testing.T) {
	oldFn := orchardWitnessWithOpsFn
	defer func() { orchardWitnessWithOpsFn = oldFn }()

	tests := []struct {
		name      string
		useShards bool
		subtree   store.OrchardSubtreeRoot
		shard     store.OrchardShardRoot
	}{
		{
			name:    "subtree corrupt end position",
			subtree: store.OrchardSubtreeRoot{SubtreeIndex: 1, EndPosition: 2 * orchardSubtreeLeafCount, EndHeight: 10, EndBlockHash: "canonical", Root: "bad-root"},
		},
		{
			name:    "subtree future end height",
			subtree: store.OrchardSubtreeRoot{SubtreeIndex: 1, EndPosition: 2*orchardSubtreeLeafCount - 1, EndHeight: 11, EndBlockHash: "canonical", Root: "bad-root"},
		},
		{
			name:      "shard corrupt end position",
			useShards: true,
			shard:     store.OrchardShardRoot{Version: 1, ShardIndex: 1, EndPosition: 2 * orchardShardLeafCount, EndHeight: 10, EndBlockHash: "canonical", Root: "bad-root"},
		},
		{
			name:      "shard future end height",
			useShards: true,
			shard:     store.OrchardShardRoot{Version: 1, ShardIndex: 1, EndPosition: 2*orchardShardLeafCount - 1, EndHeight: 11, EndBlockHash: "canonical", Root: "bad-root"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			groupLeafCount := orchardSubtreeLeafCount
			if tc.useShards {
				groupLeafCount = orchardShardLeafCount
			}
			var gotOps []orchardscan.WitnessOperation
			orchardWitnessWithOpsFn = func(_ context.Context, _ uint32, _ []orchardscan.WitnessTarget, ops []orchardscan.WitnessOperation) (orchardscan.WitnessResult, error) {
				gotOps = append([]orchardscan.WitnessOperation(nil), ops...)
				return orchardscan.WitnessResult{Root: "root", Paths: []orchardscan.WitnessPath{{Position: 0}}}, nil
			}
			st := &fakeWitnessFastStore{
				treeSize:     2 * groupLeafCount,
				targetByPos:  map[int64]store.OrchardCommitment{0: {Position: 0, Height: 10, CMX: "target"}},
				rootByIndex:  map[int64]store.OrchardSubtreeRoot{},
				shardByIndex: map[int64]store.OrchardShardRoot{},
			}
			if tc.useShards {
				st.shardByIndex[1] = tc.shard
			} else {
				st.rootByIndex[1] = tc.subtree
			}

			if _, err := computeOrchardWitnessCached(context.Background(), st, 10, []uint32{0}, tc.useShards); err != nil {
				t.Fatal(err)
			}
			if len(st.rangeCalls) != 2 {
				t.Fatalf("range calls=%d want 2 (invalid cached root must fall back to leaves)", len(st.rangeCalls))
			}
			for _, op := range gotOps {
				if len(op.SubtreeRoots) != 0 || len(op.ShardRoots) != 0 {
					t.Fatalf("invalid cached root was inserted: %+v", op)
				}
			}
		})
	}
}

func TestComputeOrchardWitnessCachedRejectsConcurrentAnchorChange(t *testing.T) {
	oldFn := orchardWitnessWithOpsFn
	defer func() { orchardWitnessWithOpsFn = oldFn }()

	computeStarted := make(chan struct{})
	anchorChanged := make(chan struct{})
	orchardWitnessWithOpsFn = func(_ context.Context, _ uint32, _ []orchardscan.WitnessTarget, _ []orchardscan.WitnessOperation) (orchardscan.WitnessResult, error) {
		close(computeStarted)
		<-anchorChanged
		return orchardscan.WitnessResult{Root: "mixed-root", Paths: []orchardscan.WitnessPath{{Position: 0}}}, nil
	}
	st := &fakeWitnessFastStore{
		treeSize:    1,
		anchor:      "old-anchor",
		targetByPos: map[int64]store.OrchardCommitment{0: {Position: 0, Height: 10, CMX: "target"}},
	}
	go func() {
		<-computeStarted
		st.mu.Lock()
		st.anchor = "new-anchor"
		st.mu.Unlock()
		close(anchorChanged)
	}()

	res, err := computeOrchardWitnessCached(context.Background(), st, 10, []uint32{0}, true)
	if !errors.Is(err, errWitnessAnchorChanged) {
		t.Fatalf("error=%v want anchor change", err)
	}
	if res.Root != "" || len(res.Paths) != 0 {
		t.Fatalf("mixed witness escaped: %+v", res)
	}
}
