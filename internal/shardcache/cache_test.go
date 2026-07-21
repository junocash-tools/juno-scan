package shardcache

import (
	"context"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

type fakeStore struct {
	tip     store.BlockTip
	cursor  int64
	version int32
	roots   map[int64]store.OrchardShardRoot
	applied []store.OrchardShardRoot
	ends    []store.OrchardCommitment
	endCall int
}

func (f *fakeStore) Tip(context.Context) (store.BlockTip, bool, error) {
	return f.tip, true, nil
}

func (f *fakeStore) HashAtHeight(context.Context, int64) (string, bool, error) {
	return "hash", true, nil
}

func (f *fakeStore) OrchardTreeSizeAtHeight(context.Context, int64) (int64, error) {
	return 2 * LeafCount, nil
}

func (f *fakeStore) ListOrchardCommitmentCMXByPositionRange(_ context.Context, _ int64, start, end int64) ([]string, error) {
	return make([]string, end-start), nil
}

func (f *fakeStore) ListOrchardCommitmentsByPositionsUpToHeight(_ context.Context, _ int64, positions []int64) ([]store.OrchardCommitment, error) {
	if len(f.ends) > 0 {
		idx := f.endCall
		if idx >= len(f.ends) {
			idx = len(f.ends) - 1
		}
		f.endCall++
		return []store.OrchardCommitment{f.ends[idx]}, nil
	}
	return []store.OrchardCommitment{{Position: positions[0], Height: 7, CMX: "end-cmx"}}, nil
}

func (f *fakeStore) ListOrchardShardRootsByIndexRange(_ context.Context, start int64, limit int) ([]store.OrchardShardRoot, error) {
	var out []store.OrchardShardRoot
	for i := start; i < start+int64(limit); i++ {
		if root, ok := f.roots[i]; ok {
			out = append(out, root)
		}
	}
	return out, nil
}

func (f *fakeStore) OrchardShardBackfillCursor(context.Context) (int32, int64, error) {
	return f.version, f.cursor, nil
}

func (f *fakeStore) SetOrchardShardBackfillCursor(_ context.Context, version int32, next int64) error {
	f.version, f.cursor = version, next
	return nil
}

func (f *fakeStore) ApplyOrchardShardRoot(_ context.Context, root store.OrchardShardRoot, next int64) error {
	f.applied = append(f.applied, root)
	f.roots[root.ShardIndex] = root
	f.cursor = next
	return nil
}

func TestRunOnceRewindsStaleCursorAndBackfillsBoundedBatch(t *testing.T) {
	f := &fakeStore{
		tip: store.BlockTip{Height: 10, Hash: "tip"}, version: Version, cursor: 2,
		roots: map[int64]store.OrchardShardRoot{
			0: {Version: Version, ShardIndex: 0, EndPosition: LeafCount - 1, EndHeight: 7, EndBlockHash: "hash", Root: "root-0"},
			1: {Version: 99, ShardIndex: 1, EndHeight: 7, EndBlockHash: "stale", Root: "root-1"},
		},
	}
	svc, err := New(f, Options{Enabled: true, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	svc.computeRoot = func(context.Context, []string) (string, error) { return "new-root", nil }
	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.cursor != 2 || len(f.applied) != 1 || f.applied[0].ShardIndex != 1 || f.applied[0].Version != Version {
		t.Fatalf("cursor=%d applied=%+v", f.cursor, f.applied)
	}
	st := svc.Snapshot()
	if st.CompleteRoots != 2 || st.RemainingRoots != 0 || st.LastError != "" {
		t.Fatalf("status=%+v", st)
	}
}

func TestRunOnceDoesNotApplyRootAcrossCanonicalIdentityChange(t *testing.T) {
	f := &fakeStore{
		tip: store.BlockTip{Height: 10, Hash: "tip"}, version: Version,
		roots: map[int64]store.OrchardShardRoot{},
		ends: []store.OrchardCommitment{
			{Position: LeafCount - 1, Height: 7, CMX: "old-end"},
			{Position: LeafCount - 1, Height: 8, CMX: "new-end"},
		},
	}
	svc, err := New(f, Options{Enabled: true, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	svc.computeRoot = func(context.Context, []string) (string, error) { return "old-root", nil }
	err = svc.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected canonical identity change error")
	}
	if len(f.applied) != 0 || f.cursor != 0 {
		t.Fatalf("stale root applied: cursor=%d roots=%+v", f.cursor, f.applied)
	}
}
