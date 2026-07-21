package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

const (
	orchardSubtreeLeafCount      int64 = 1 << 16
	orchardShardLeafCount        int64 = 1 << 12
	witnessAppendBatchSize             = 5000
	witnessSubtreeRootBatchSize        = 256
	witnessSubtreeRootChunkSize        = 2048
	witnessFastMaxStreamSubtrees       = 8
)

var (
	errNoCommitments              = errors.New("api: no commitments")
	errInvalidCommitmentPositions = errors.New("api: invalid commitment positions")
	errDBAccess                   = errors.New("api: db access")
	errPreferLegacyWitnessPath    = errors.New("api: prefer legacy witness path")

	orchardWitnessWithOpsFn = orchardscan.OrchardWitnessWithOps
)

type orchardWitnessFastStore interface {
	HashAtHeight(ctx context.Context, height int64) (string, bool, error)
	OrchardTreeSizeAtHeight(ctx context.Context, height int64) (int64, error)
	ListOrchardCommitmentCMXByPositionRange(ctx context.Context, anchorHeight, startPos, endPos int64) ([]string, error)
	ListOrchardCommitmentsByPositionsUpToHeight(ctx context.Context, anchorHeight int64, positions []int64) ([]store.OrchardCommitment, error)
	ListOrchardSubtreeRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardSubtreeRoot, error)
	ListOrchardShardRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardShardRoot, error)
}

func (s *Server) computeOrchardWitness(ctx context.Context, anchorHeight int64, positions []uint32) (orchardscan.WitnessResult, error) {
	if s.witnessMode == "legacy" {
		return computeOrchardWitnessLegacy(ctx, s.st, anchorHeight, positions)
	}
	fs, ok := s.st.(orchardWitnessFastStore)
	if !ok {
		return computeOrchardWitnessLegacy(ctx, s.st, anchorHeight, positions)
	}
	if s.witnessMode == "shard" {
		return computeOrchardWitnessCached(ctx, fs, anchorHeight, positions, true)
	}
	if s.witnessMode == "subtree" {
		return computeOrchardWitnessCached(ctx, fs, anchorHeight, positions, false)
	}
	shardErr := error(nil)
	if res, err := computeOrchardWitnessCached(ctx, fs, anchorHeight, positions, true); err == nil {
		return res, nil
	} else if ctx.Err() != nil {
		return orchardscan.WitnessResult{}, ctx.Err()
	} else {
		shardErr = err
	}
	if res, err := computeOrchardWitnessCached(ctx, fs, anchorHeight, positions, false); err == nil {
		res.FallbackFrom = "shard"
		res.FallbackReason = witnessFallbackReason(shardErr)
		return res, nil
	} else if ctx.Err() != nil {
		return orchardscan.WitnessResult{}, ctx.Err()
	}
	res, err := computeOrchardWitnessLegacy(ctx, s.st, anchorHeight, positions)
	if err == nil {
		res.FallbackFrom = "shard,subtree"
		res.FallbackReason = witnessFallbackReason(shardErr)
	}
	return res, err
}

func witnessFallbackReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errPreferLegacyWitnessPath) {
		return "cache_incomplete"
	}
	return "fast_path_error"
}

func computeOrchardWitnessLegacy(ctx context.Context, st store.Store, anchorHeight int64, positions []uint32) (orchardscan.WitnessResult, error) {
	commitments, err := st.ListOrchardCommitmentsUpToHeight(ctx, anchorHeight)
	if err != nil {
		return orchardscan.WitnessResult{}, fmt.Errorf("%w: list commitments: %v", errDBAccess, err)
	}
	if len(commitments) == 0 {
		return orchardscan.WitnessResult{}, errNoCommitments
	}

	var expectedPos int64
	cmxHex := make([]string, 0, len(commitments))
	for _, c := range commitments {
		if c.Position != expectedPos {
			return orchardscan.WitnessResult{}, errInvalidCommitmentPositions
		}
		expectedPos++
		cmxHex = append(cmxHex, c.CMX)
	}

	res, err := orchardscan.OrchardWitness(ctx, cmxHex, positions)
	res.ComputeMode = "legacy"
	res.StreamedLeafCount = int64(len(cmxHex))
	return res, err
}

func computeOrchardWitnessFast(ctx context.Context, st orchardWitnessFastStore, anchorHeight int64, positions []uint32) (orchardscan.WitnessResult, error) {
	return computeOrchardWitnessCached(ctx, st, anchorHeight, positions, false)
}

func computeOrchardWitnessCached(ctx context.Context, st orchardWitnessFastStore, anchorHeight int64, positions []uint32, useShards bool) (orchardscan.WitnessResult, error) {
	if anchorHeight < 0 || anchorHeight > int64(^uint32(0)) {
		return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
	}

	leafCount, err := st.OrchardTreeSizeAtHeight(ctx, anchorHeight)
	if err != nil {
		return orchardscan.WitnessResult{}, fmt.Errorf("%w: orchard tree size: %v", errDBAccess, err)
	}
	if leafCount <= 0 {
		return orchardscan.WitnessResult{}, errNoCommitments
	}
	if leafCount > int64(^uint32(0))+1 {
		return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
	}

	wantPositions := make([]int64, 0, len(positions))
	seen := make(map[uint32]struct{}, len(positions))
	for _, p := range positions {
		if _, ok := seen[p]; ok {
			return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
		}
		seen[p] = struct{}{}
		if int64(p) >= leafCount {
			return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
		}
		wantPositions = append(wantPositions, int64(p))
	}

	targetCommitments, err := st.ListOrchardCommitmentsByPositionsUpToHeight(ctx, anchorHeight, wantPositions)
	if err != nil {
		return orchardscan.WitnessResult{}, fmt.Errorf("%w: list target commitments: %v", errDBAccess, err)
	}
	if len(targetCommitments) != len(wantPositions) {
		return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
	}

	targetCMXByPos := make(map[int64]string, len(targetCommitments))
	for _, c := range targetCommitments {
		targetCMXByPos[c.Position] = c.CMX
	}
	targets := make([]orchardscan.WitnessTarget, 0, len(positions))
	for _, p := range positions {
		cmxHex, ok := targetCMXByPos[int64(p)]
		if !ok {
			return orchardscan.WitnessResult{}, &orchardscan.Error{Code: orchardscan.ErrInvalidRequest}
		}
		targets = append(targets, orchardscan.WitnessTarget{
			Position: p,
			CMXHex:   cmxHex,
		})
	}

	rootByIndex := make(map[int64]string)
	groupLeafCount := orchardSubtreeLeafCount
	maxStreamGroups := witnessFastMaxStreamSubtrees
	if useShards {
		groupLeafCount = orchardShardLeafCount
		maxStreamGroups *= int(orchardSubtreeLeafCount / orchardShardLeafCount)
	}
	fullSubtrees := leafCount / groupLeafCount
	for start := int64(0); start < fullSubtrees; start += witnessSubtreeRootChunkSize {
		limit := int(fullSubtrees - start)
		if limit > witnessSubtreeRootChunkSize {
			limit = witnessSubtreeRootChunkSize
		}
		if useShards {
			roots, err := st.ListOrchardShardRootsByIndexRange(ctx, start, limit)
			if err != nil {
				return orchardscan.WitnessResult{}, fmt.Errorf("%w: list shard roots: %v", errDBAccess, err)
			}
			for _, root := range roots {
				expectedEndPosition := (root.ShardIndex+1)*groupLeafCount - 1
				if root.Version != 1 || root.ShardIndex < start || root.ShardIndex >= start+int64(limit) || root.Root == "" || root.EndPosition != expectedEndPosition || root.EndHeight < 0 || root.EndHeight > anchorHeight {
					continue
				}
				canonical, ok, err := st.HashAtHeight(ctx, root.EndHeight)
				if err != nil {
					return orchardscan.WitnessResult{}, fmt.Errorf("%w: validate shard root: %v", errDBAccess, err)
				}
				if !ok || canonical != root.EndBlockHash {
					continue
				}
				rootByIndex[root.ShardIndex] = root.Root
			}
		} else {
			roots, err := st.ListOrchardSubtreeRootsByIndexRange(ctx, start, limit)
			if err != nil {
				return orchardscan.WitnessResult{}, fmt.Errorf("%w: list subtree roots: %v", errDBAccess, err)
			}
			for _, root := range roots {
				expectedEndPosition := (root.SubtreeIndex+1)*groupLeafCount - 1
				if root.SubtreeIndex < start || root.SubtreeIndex >= start+int64(limit) || root.Root == "" || root.EndPosition != expectedEndPosition || root.EndHeight < 0 || root.EndHeight > anchorHeight {
					continue
				}
				canonical, ok, err := st.HashAtHeight(ctx, root.EndHeight)
				if err != nil {
					return orchardscan.WitnessResult{}, fmt.Errorf("%w: validate subtree root: %v", errDBAccess, err)
				}
				if !ok || canonical != root.EndBlockHash {
					continue
				}
				rootByIndex[root.SubtreeIndex] = root.Root
			}
		}
	}

	targetedSubtrees := make(map[int64]struct{}, len(positions))
	for _, p := range positions {
		subtreeIndex := int64(p) / groupLeafCount
		if subtreeIndex < fullSubtrees {
			targetedSubtrees[subtreeIndex] = struct{}{}
		}
	}
	rootedNonTargetedSubtrees := 0
	for subtreeIndex := range rootByIndex {
		if subtreeIndex < 0 || subtreeIndex >= fullSubtrees {
			continue
		}
		if _, targeted := targetedSubtrees[subtreeIndex]; targeted {
			continue
		}
		rootedNonTargetedSubtrees++
	}
	subtreesToStream := int(fullSubtrees) - rootedNonTargetedSubtrees
	if subtreesToStream > maxStreamGroups {
		return orchardscan.WitnessResult{}, errPreferLegacyWitnessPath
	}

	ops := make([]orchardscan.WitnessOperation, 0, 32)
	rootBatch := make([]string, 0, witnessSubtreeRootBatchSize)
	insertedRootCount := 0
	streamedLeafCount := int64(0)

	flushRootBatch := func() {
		if len(rootBatch) == 0 {
			return
		}
		op := orchardscan.WitnessOperation{Type: orchardscan.WitnessOpInsertSubtreeRoots, SubtreeRoots: append([]string(nil), rootBatch...)}
		if useShards {
			op = orchardscan.WitnessOperation{Type: orchardscan.WitnessOpInsertShardRoots, ShardRoots: append([]string(nil), rootBatch...)}
		}
		ops = append(ops, op)
		insertedRootCount += len(rootBatch)
		rootBatch = rootBatch[:0]
	}

	appendRangeAsBatches := func(startPos, endPos int64) error {
		cmxHex, err := st.ListOrchardCommitmentCMXByPositionRange(ctx, anchorHeight, startPos, endPos)
		if err != nil {
			return fmt.Errorf("%w: list commitment range: %v", errDBAccess, err)
		}
		if int64(len(cmxHex)) != endPos-startPos {
			return errInvalidCommitmentPositions
		}
		streamedLeafCount += int64(len(cmxHex))
		for offset := 0; offset < len(cmxHex); offset += witnessAppendBatchSize {
			end := offset + witnessAppendBatchSize
			if end > len(cmxHex) {
				end = len(cmxHex)
			}
			ops = append(ops, orchardscan.WitnessOperation{
				Type:   orchardscan.WitnessOpAppendBatch,
				CMXHex: append([]string(nil), cmxHex[offset:end]...),
			})
		}
		return nil
	}

	for subtreeIndex := int64(0); subtreeIndex < fullSubtrees; subtreeIndex++ {
		startPos := subtreeIndex * groupLeafCount
		endPos := startPos + groupLeafCount

		_, targeted := targetedSubtrees[subtreeIndex]
		rootHex, hasRoot := rootByIndex[subtreeIndex]
		if !targeted && hasRoot {
			rootBatch = append(rootBatch, rootHex)
			if len(rootBatch) >= witnessSubtreeRootBatchSize {
				flushRootBatch()
			}
			continue
		}

		flushRootBatch()
		if err := appendRangeAsBatches(startPos, endPos); err != nil {
			return orchardscan.WitnessResult{}, err
		}
	}
	flushRootBatch()

	remainderStart := fullSubtrees * groupLeafCount
	if remainderStart < leafCount {
		if err := appendRangeAsBatches(remainderStart, leafCount); err != nil {
			return orchardscan.WitnessResult{}, err
		}
	}

	res, err := orchardWitnessWithOpsFn(ctx, uint32(anchorHeight), targets, ops)
	if useShards {
		res.ComputeMode = "shard"
	} else {
		res.ComputeMode = "subtree"
	}
	res.StreamedLeafCount = streamedLeafCount
	res.InsertedRootCount = insertedRootCount
	return res, err
}
