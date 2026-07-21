package shardcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

const (
	Version   int32 = 1
	LeafCount int64 = 1 << 12
)

type Store interface {
	Tip(context.Context) (store.BlockTip, bool, error)
	HashAtHeight(context.Context, int64) (string, bool, error)
	OrchardTreeSizeAtHeight(context.Context, int64) (int64, error)
	ListOrchardCommitmentCMXByPositionRange(context.Context, int64, int64, int64) ([]string, error)
	ListOrchardCommitmentsByPositionsUpToHeight(context.Context, int64, []int64) ([]store.OrchardCommitment, error)
	ListOrchardShardRootsByIndexRange(context.Context, int64, int) ([]store.OrchardShardRoot, error)
	OrchardShardBackfillCursor(context.Context) (int32, int64, error)
	SetOrchardShardBackfillCursor(context.Context, int32, int64) error
	ApplyOrchardShardRoot(context.Context, store.OrchardShardRoot, int64) error
}

type Options struct {
	Enabled      bool
	BatchSize    int
	PollInterval time.Duration
	Yield        time.Duration
	NodeHeight   func() (int64, bool)
}

type Status struct {
	Enabled         bool   `json:"enabled"`
	Version         int32  `json:"version"`
	LeafCount       int64  `json:"leaf_count"`
	NextIndex       int64  `json:"next_index"`
	CompleteRoots   int64  `json:"complete_roots"`
	RemainingRoots  int64  `json:"remaining_roots"`
	Running         bool   `json:"running"`
	LastError       string `json:"last_error,omitempty"`
	LastRunUnix     int64  `json:"last_run_unix,omitempty"`
	LastSuccessUnix int64  `json:"last_success_unix,omitempty"`
}

type Service struct {
	store       Store
	opts        Options
	now         func() time.Time
	computeRoot func(context.Context, []string) (string, error)

	mu     sync.RWMutex
	status Status
}

func New(st Store, opts Options) (*Service, error) {
	if st == nil {
		return nil, errors.New("shardcache: store is nil")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1
	}
	if opts.BatchSize > 64 {
		return nil, errors.New("shardcache: batch size out of range")
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Second
	}
	return &Service{
		store:       st,
		opts:        opts,
		now:         time.Now,
		computeRoot: orchardscan.OrchardShardRoot,
		status:      Status{Enabled: opts.Enabled, Version: Version, LeafCount: LeafCount},
	}, nil
}

func (s *Service) Snapshot() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *Service) Run(ctx context.Context) error {
	if !s.opts.Enabled {
		return nil
	}
	s.setRunning(true)
	defer s.setRunning(false)
	if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
		s.recordError(err)
	}
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
				s.recordError(err)
			}
		}
	}
}

func (s *Service) RunOnce(ctx context.Context) error {
	now := s.now()
	st := s.Snapshot()
	st.LastRunUnix = now.Unix()

	tip, ok, err := s.store.Tip(ctx)
	if err != nil {
		return err
	}
	if !ok {
		st.NextIndex, st.CompleteRoots, st.RemainingRoots = 0, 0, 0
		s.storeStatus(st)
		return nil
	}
	if s.opts.NodeHeight != nil {
		if nodeHeight, known := s.opts.NodeHeight(); known && tip.Height < nodeHeight {
			st.LastError = ""
			s.storeStatus(st)
			return nil
		}
	}
	leafCount, err := s.store.OrchardTreeSizeAtHeight(ctx, tip.Height)
	if err != nil {
		return err
	}
	fullShards := leafCount / LeafCount
	version, cursor, err := s.store.OrchardShardBackfillCursor(ctx)
	if err != nil {
		return err
	}
	if version != Version || cursor < 0 {
		cursor = 0
		if err := s.store.SetOrchardShardBackfillCursor(ctx, Version, cursor); err != nil {
			return err
		}
	}
	cursor, err = s.rewindStaleCursor(ctx, cursor, tip.Height)
	if err != nil {
		return err
	}
	if cursor > fullShards {
		cursor = fullShards
		if err := s.store.SetOrchardShardBackfillCursor(ctx, Version, cursor); err != nil {
			return err
		}
	}
	st.NextIndex = cursor
	st.CompleteRoots = cursor
	st.RemainingRoots = fullShards - cursor

	upper := cursor + int64(s.opts.BatchSize)
	if upper > fullShards {
		upper = fullShards
	}
	for index := cursor; index < upper; index++ {
		if err := s.processShard(ctx, tip.Height, index); err != nil {
			return err
		}
		st.NextIndex = index + 1
		st.CompleteRoots = index + 1
		st.RemainingRoots = fullShards - st.NextIndex
		if s.opts.Yield > 0 && index+1 < upper {
			timer := time.NewTimer(s.opts.Yield)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	st.LastError = ""
	st.LastSuccessUnix = s.now().Unix()
	s.storeStatus(st)
	return nil
}

func (s *Service) rewindStaleCursor(ctx context.Context, cursor, tipHeight int64) (int64, error) {
	rewound := cursor
	for rewound > 0 {
		roots, err := s.store.ListOrchardShardRootsByIndexRange(ctx, rewound-1, 1)
		if err != nil {
			return 0, err
		}
		expectedEnd := rewound*LeafCount - 1
		if len(roots) != 1 || roots[0].ShardIndex != rewound-1 || roots[0].Version != Version || roots[0].EndPosition != expectedEnd || roots[0].EndHeight < 0 || roots[0].EndHeight > tipHeight {
			rewound--
			continue
		}
		canonical, ok, err := s.store.HashAtHeight(ctx, roots[0].EndHeight)
		if err != nil {
			return 0, err
		}
		if !ok || canonical != roots[0].EndBlockHash {
			rewound--
			continue
		}
		break
	}
	if rewound != cursor {
		if err := s.store.SetOrchardShardBackfillCursor(ctx, Version, rewound); err != nil {
			return 0, err
		}
	}
	return rewound, nil
}

func (s *Service) processShard(ctx context.Context, anchorHeight, index int64) error {
	start := index * LeafCount
	end := start + LeafCount
	before, err := s.endCommitmentIdentity(ctx, anchorHeight, index, end-1)
	if err != nil {
		return err
	}
	cmx, err := s.store.ListOrchardCommitmentCMXByPositionRange(ctx, anchorHeight, start, end)
	if err != nil {
		return fmt.Errorf("shardcache: commitments: %w", err)
	}
	if int64(len(cmx)) != LeafCount {
		return fmt.Errorf("shardcache: incomplete shard %d", index)
	}
	root, err := s.computeRoot(ctx, cmx)
	if err != nil {
		return fmt.Errorf("shardcache: compute root: %w", err)
	}
	after, err := s.endCommitmentIdentity(ctx, anchorHeight, index, end-1)
	if err != nil {
		return err
	}
	if before.Position != after.Position || before.Height != after.Height || before.CMX != after.CMX || before.BlockHash != after.BlockHash {
		return fmt.Errorf("shardcache: canonical identity changed while computing shard %d", index)
	}
	return s.store.ApplyOrchardShardRoot(ctx, store.OrchardShardRoot{
		Version: Version, ShardIndex: index, EndPosition: end - 1,
		EndHeight: before.Height, EndBlockHash: before.BlockHash, Root: root,
	}, index+1)
}

type endCommitmentIdentity struct {
	Position  int64
	Height    int64
	CMX       string
	BlockHash string
}

func (s *Service) endCommitmentIdentity(ctx context.Context, anchorHeight, index, position int64) (endCommitmentIdentity, error) {
	ends, err := s.store.ListOrchardCommitmentsByPositionsUpToHeight(ctx, anchorHeight, []int64{position})
	if err != nil || len(ends) != 1 || ends[0].Position != position || ends[0].Height < 0 || ends[0].Height > anchorHeight || ends[0].CMX == "" {
		return endCommitmentIdentity{}, fmt.Errorf("shardcache: end commitment missing for shard %d", index)
	}
	endHash, ok, err := s.store.HashAtHeight(ctx, ends[0].Height)
	if err != nil || !ok || endHash == "" {
		return endCommitmentIdentity{}, fmt.Errorf("shardcache: canonical hash missing at %d", ends[0].Height)
	}
	return endCommitmentIdentity{Position: ends[0].Position, Height: ends[0].Height, CMX: ends[0].CMX, BlockHash: endHash}, nil
}

func (s *Service) setRunning(running bool) {
	s.mu.Lock()
	s.status.Running = running
	s.mu.Unlock()
}

func (s *Service) recordError(err error) {
	s.mu.Lock()
	s.status.LastError = err.Error()
	s.status.LastRunUnix = s.now().Unix()
	s.mu.Unlock()
}

func (s *Service) storeStatus(st Status) {
	s.mu.Lock()
	st.Enabled = s.opts.Enabled
	st.Running = s.status.Running
	s.status = st
	s.mu.Unlock()
}
