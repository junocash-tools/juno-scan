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
	OrchardTreeSizeAtHeight(ctx context.Context, height int64) (int64, error)
	ListOrchardCommitmentCMXByPositionRange(ctx context.Context, anchorHeight, startPos, endPos int64) ([]string, error)
	ListOrchardCommitmentsByPositionsUpToHeight(ctx context.Context, anchorHeight int64, positions []int64) ([]store.OrchardCommitment, error)
	ListOrchardSubtreeRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardSubtreeRoot, error)
}

func (s *Server) computeOrchardWitness(ctx context.Context, anchorHeight int64, positions []uint32) (orchardscan.WitnessResult, error) {
	if fs, ok := s.st.(orchardWitnessFastStore); ok {
		res, err := computeOrchardWitnessFast(ctx, fs, anchorHeight, positions)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, errPreferLegacyWitnessPath) {
			return computeOrchardWitnessLegacy(ctx, s.st, anchorHeight, positions)
		}
		return orchardscan.WitnessResult{}, err
	}
	return computeOrchardWitnessLegacy(ctx, s.st, anchorHeight, positions)
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

	return orchardscan.OrchardWitness(ctx, cmxHex, positions)
}

func computeOrchardWitnessFast(ctx context.Context, st orchardWitnessFastStore, anchorHeight int64, positions []uint32) (orchardscan.WitnessResult, error) {
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
	fullSubtrees := leafCount / orchardSubtreeLeafCount
	for start := int64(0); start < fullSubtrees; start += witnessSubtreeRootChunkSize {
		limit := int(fullSubtrees - start)
		if limit > witnessSubtreeRootChunkSize {
			limit = witnessSubtreeRootChunkSize
		}
		roots, err := st.ListOrchardSubtreeRootsByIndexRange(ctx, start, limit)
		if err != nil {
			return orchardscan.WitnessResult{}, fmt.Errorf("%w: list subtree roots: %v", errDBAccess, err)
		}
		for _, root := range roots {
			if root.SubtreeIndex < start {
				continue
			}
			if root.SubtreeIndex >= start+int64(limit) {
				continue
			}
			if root.Root == "" {
				continue
			}
			rootByIndex[root.SubtreeIndex] = root.Root
		}
	}

	targetedSubtrees := make(map[int64]struct{}, len(positions))
	for _, p := range positions {
		subtreeIndex := int64(p) / orchardSubtreeLeafCount
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
	if subtreesToStream > witnessFastMaxStreamSubtrees {
		return orchardscan.WitnessResult{}, errPreferLegacyWitnessPath
	}

	ops := make([]orchardscan.WitnessOperation, 0, 32)
	rootBatch := make([]string, 0, witnessSubtreeRootBatchSize)

	flushRootBatch := func() {
		if len(rootBatch) == 0 {
			return
		}
		ops = append(ops, orchardscan.WitnessOperation{
			Type:         orchardscan.WitnessOpInsertSubtreeRoots,
			SubtreeRoots: append([]string(nil), rootBatch...),
		})
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
		startPos := subtreeIndex * orchardSubtreeLeafCount
		endPos := startPos + orchardSubtreeLeafCount

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

	remainderStart := fullSubtrees * orchardSubtreeLeafCount
	if remainderStart < leafCount {
		if err := appendRangeAsBatches(remainderStart, leafCount); err != nil {
			return orchardscan.WitnessResult{}, err
		}
	}

	return orchardWitnessWithOpsFn(ctx, uint32(anchorHeight), targets, ops)
}
