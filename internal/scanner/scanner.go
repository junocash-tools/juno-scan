package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/zmq"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/Abdullah1738/juno-sdk-go/types"
)

type Scanner struct {
	st    store.Store
	rpc   *sdkjunocashd.Client
	uaHRP string

	pollInterval  time.Duration
	confirmations int64

	zmqHashBlockEndpoint string

	mempoolOrchardNullifiersByTxID map[string][]string
	mempoolTxExpiryHeightByTxID    map[string]*int64

	statusMu        sync.RWMutex
	nodeHeight      int64
	nodeHeightKnown bool
	mempoolRefresh  MempoolRefreshStatus
}

type MempoolRefreshStatus struct {
	Ready      bool
	EventEpoch string
	Height     int64
	Hash       string
}

type Status struct {
	NodeHeight      int64
	NodeHeightKnown bool
	MempoolRefresh  MempoolRefreshStatus
}

func (s *Scanner) Status() Status {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return Status{
		NodeHeight:      s.nodeHeight,
		NodeHeightKnown: s.nodeHeightKnown,
		MempoolRefresh:  s.mempoolRefresh,
	}
}

func (s *Scanner) recordNodeHeight(height int64) {
	s.statusMu.Lock()
	s.nodeHeight = height
	s.nodeHeightKnown = true
	s.statusMu.Unlock()
}

func (s *Scanner) recordMempoolRefresh(eventEpoch string, tip store.BlockTip) {
	s.statusMu.Lock()
	s.mempoolRefresh = MempoolRefreshStatus{
		Ready:      true,
		EventEpoch: eventEpoch,
		Height:     tip.Height,
		Hash:       tip.Hash,
	}
	s.statusMu.Unlock()
}

func (s *Scanner) clearMempoolRefresh() {
	s.statusMu.Lock()
	s.mempoolRefresh = MempoolRefreshStatus{}
	s.statusMu.Unlock()
}

const (
	orchardSubtreeLeafCount       int64 = 1 << 16
	orchardSubtreeRootChunkSize         = 2048
	orchardSubtreeBackfillPerScan       = 2
)

type orchardSubtreeRootTx interface {
	ListOrchardCommitmentCMXByPositionRange(ctx context.Context, startPos, endPos int64) ([]string, error)
	InsertOrchardSubtreeRoot(ctx context.Context, r store.OrchardSubtreeRoot) error
}

type orchardSubtreeRootBackfillStore interface {
	OrchardTreeSizeAtHeight(ctx context.Context, height int64) (int64, error)
	ListOrchardCommitmentCMXByPositionRange(ctx context.Context, anchorHeight, startPos, endPos int64) ([]string, error)
	ListOrchardCommitmentsByPositionsUpToHeight(ctx context.Context, anchorHeight int64, positions []int64) ([]store.OrchardCommitment, error)
	ListOrchardSubtreeRootsByIndexRange(ctx context.Context, startIndex int64, limit int) ([]store.OrchardSubtreeRoot, error)
}

func New(st store.Store, rpc *sdkjunocashd.Client, uaHRP string, pollInterval time.Duration, confirmations int64, zmqHashBlockEndpoint string) (*Scanner, error) {
	if st == nil {
		return nil, errors.New("scanner: store is nil")
	}
	if rpc == nil {
		return nil, errors.New("scanner: rpc is nil")
	}
	if uaHRP == "" {
		return nil, errors.New("scanner: ua hrp is required")
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if confirmations <= 0 {
		confirmations = 100
	}
	if zmqHashBlockEndpoint != "" {
		if _, err := zmq.ParseEndpoint(zmqHashBlockEndpoint); err != nil {
			return nil, fmt.Errorf("scanner: zmq-hashblock: %w", err)
		}
	}
	return &Scanner{
		st:                   st,
		rpc:                  rpc,
		uaHRP:                uaHRP,
		pollInterval:         pollInterval,
		confirmations:        confirmations,
		zmqHashBlockEndpoint: zmqHashBlockEndpoint,
	}, nil
}

func (s *Scanner) Run(ctx context.Context) error {
	if tip, ok, err := s.st.Tip(ctx); err != nil {
		return err
	} else if ok {
		if err := reconcileCompleteWalletBackfillProgress(ctx, s.st, tip.Height); err != nil {
			return err
		}
	}

	var zmqNotify <-chan struct{}
	if s.zmqHashBlockEndpoint != "" {
		ch := make(chan struct{}, 1)
		zmqNotify = ch
		go func() {
			_ = zmq.Notify(ctx, zmq.NotifyConfig{
				Endpoint: s.zmqHashBlockEndpoint,
				Topic:    "hashblock",
			}, ch, log.Printf)
		}()
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if err := s.scanOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-zmqNotify:
		}
	}
}

func (s *Scanner) scanOnce(ctx context.Context) error {
	for ctx.Err() == nil {
		tip, ok, err := s.st.Tip(ctx)
		if err != nil {
			return err
		}

		nextHeight := int64(0)
		if ok {
			nextHeight = tip.Height + 1
		}

		chainHeight, err := s.rpc.GetBlockCount(ctx)
		if err != nil {
			return fmt.Errorf("scanner: getblockcount: %w", err)
		}
		s.recordNodeHeight(chainHeight)
		if ok {
			rolledBack, err := s.reconcileCanonicalTip(ctx, tip, chainHeight)
			if err != nil {
				return err
			}
			if rolledBack {
				continue
			}
		}
		if nextHeight > chainHeight {
			if err := s.backfillMissingSubtreeRoots(ctx, chainHeight, orchardSubtreeBackfillPerScan); err != nil {
				return err
			}
			if err := s.updatePendingSpends(ctx, chainHeight); err != nil {
				return err
			}
			return nil
		}

		nextHash, err := s.rpc.GetBlockHash(ctx, nextHeight)
		if err != nil {
			return fmt.Errorf("scanner: getblockhash(%d): %w", nextHeight, err)
		}

		var blk blockVerbose2
		if err := s.rpc.Call(ctx, "getblock", []any{nextHash, 2}, &blk); err != nil {
			return fmt.Errorf("scanner: getblock(%d): %w", nextHeight, err)
		}
		if blk.Height != nextHeight {
			return fmt.Errorf("scanner: daemon returned unexpected height: got %d want %d", blk.Height, nextHeight)
		}
		if ok && blk.PreviousBlockHash != tip.Hash {
			log.Printf("canonical tip changed before block %d commit; retrying", nextHeight)
			continue
		}

		if err := s.processBlock(ctx, blk); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (s *Scanner) reconcileCanonicalTip(ctx context.Context, tip store.BlockTip, chainHeight int64) (bool, error) {
	if tip.Height < 0 {
		return false, nil
	}

	ancestorSearchHeight := tip.Height
	if tip.Height > chainHeight {
		ancestorSearchHeight = chainHeight
	} else {
		daemonTipHash, err := s.rpc.GetBlockHash(ctx, tip.Height)
		if err != nil {
			return false, fmt.Errorf("scanner: getblockhash(%d) for canonical tip: %w", tip.Height, err)
		}
		if daemonTipHash == tip.Hash {
			return false, nil
		}
		ancestorSearchHeight = tip.Height - 1
	}

	common, err := s.findCommonAncestor(ctx, ancestorSearchHeight)
	if err != nil {
		return false, err
	}
	log.Printf("reorg detected: rolling back from height %d to height %d", tip.Height, common)
	if err := s.st.RollbackToHeight(ctx, common); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Scanner) updatePendingSpends(ctx context.Context, chainHeight int64) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	refreshSucceeded := false
	defer func() {
		if !refreshSucceeded {
			s.clearMempoolRefresh()
		}
	}()

	seenAt := time.Now().UTC()

	var mempoolSnapshot []string
	if err := s.rpc.Call(ctx, "getrawmempool", nil, &mempoolSnapshot); err != nil {
		return fmt.Errorf("scanner: getrawmempool: %w", err)
	}

	type rawTx struct {
		Hex          string `json:"hex"`
		ExpiryHeight int64  `json:"expiryheight"`
		Orchard      struct {
			Actions []struct {
				Nullifier string `json:"nullifier"`
			} `json:"actions"`
		} `json:"orchard"`
	}

	if s.mempoolOrchardNullifiersByTxID == nil {
		s.mempoolOrchardNullifiersByTxID = make(map[string][]string)
	}
	if s.mempoolTxExpiryHeightByTxID == nil {
		s.mempoolTxExpiryHeightByTxID = make(map[string]*int64)
	}

	var newTxHex map[string]string
	var newTxExpiryHeight map[string]*int64
	decodedSnapshot := false
	const maxMempoolSnapshotAttempts = 3
	for attempt := 0; attempt < maxMempoolSnapshotAttempts; attempt++ {
		orderedTxIDs, _, err := validateMempoolTxIDs(mempoolSnapshot)
		if err != nil {
			return err
		}

		nullifiersByTxID := make(map[string][]string, len(orderedTxIDs))
		expiryHeightByTxID := make(map[string]*int64, len(orderedTxIDs))
		decodedHex := make(map[string]string)
		decodedExpiry := make(map[string]*int64)
		retrySnapshot := false

		for _, txid := range orderedTxIDs {
			cachedNullifiers, nullifiersCached := s.mempoolOrchardNullifiersByTxID[txid]
			cachedExpiry, expiryCached := s.mempoolTxExpiryHeightByTxID[txid]
			if nullifiersCached && expiryCached {
				nullifiersByTxID[txid] = cachedNullifiers
				expiryHeightByTxID[txid] = cachedExpiry
				continue
			}

			// An older marker for the same chain tip cannot attest a newly observed
			// transaction until its complete nullifier set has been decoded.
			s.clearMempoolRefresh()
			var tx rawTx
			if err := s.rpc.Call(ctx, "getrawtransaction", []any{txid, 1}, &tx); err != nil {
				var rpcErr *sdkjunocashd.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == -5 {
					var revalidatedSnapshot []string
					if revalidateErr := s.rpc.Call(ctx, "getrawmempool", nil, &revalidatedSnapshot); revalidateErr != nil {
						return fmt.Errorf("scanner: revalidate mempool after getrawtransaction(%s) -5: %w", txid, revalidateErr)
					}
					_, revalidatedMempool, validateErr := validateMempoolTxIDs(revalidatedSnapshot)
					if validateErr != nil {
						return fmt.Errorf("scanner: revalidate mempool after getrawtransaction(%s) -5: %w", txid, validateErr)
					}
					if _, stillPresent := revalidatedMempool[txid]; stillPresent {
						return fmt.Errorf("scanner: getrawtransaction(%s) returned -5 but transaction remains in mempool", txid)
					}
					mempoolSnapshot = revalidatedSnapshot
					retrySnapshot = true
					break
				}
				return fmt.Errorf("scanner: getrawtransaction(%s): %w", txid, err)
			}

			nfs := make([]string, 0, len(tx.Orchard.Actions))
			seenNullifiers := make(map[string]struct{}, len(tx.Orchard.Actions))
			for _, action := range tx.Orchard.Actions {
				nf := action.Nullifier
				if !isCanonicalLowerHex64(nf) {
					return fmt.Errorf("scanner: getrawtransaction(%s) returned non-canonical Orchard nullifier", txid)
				}
				if _, duplicate := seenNullifiers[nf]; duplicate {
					return fmt.Errorf("scanner: getrawtransaction(%s) returned duplicate Orchard nullifier %s", txid, nf)
				}
				seenNullifiers[nf] = struct{}{}
				nfs = append(nfs, nf)
			}
			nullifiersByTxID[txid] = nfs

			var expPtr *int64
			if tx.ExpiryHeight > 0 {
				exp := tx.ExpiryHeight
				expPtr = &exp
			}
			expiryHeightByTxID[txid] = expPtr
			decodedExpiry[txid] = expPtr

			if txHex := strings.TrimSpace(tx.Hex); txHex != "" {
				decodedHex[txid] = txHex
			}
		}
		if retrySnapshot {
			continue
		}

		nullifierOwners := make(map[string]string)
		for _, txid := range orderedTxIDs {
			nfs, ok := nullifiersByTxID[txid]
			if !ok {
				return fmt.Errorf("scanner: incomplete mempool cache for transaction %s", txid)
			}
			if _, ok := expiryHeightByTxID[txid]; !ok {
				return fmt.Errorf("scanner: incomplete mempool expiry cache for transaction %s", txid)
			}
			for _, nf := range nfs {
				if !isCanonicalLowerHex64(nf) {
					return fmt.Errorf("scanner: cached non-canonical Orchard nullifier for transaction %s", txid)
				}
				if owner, duplicate := nullifierOwners[nf]; duplicate {
					return fmt.Errorf("scanner: mempool transactions %s and %s claim Orchard nullifier %s", owner, txid, nf)
				}
				nullifierOwners[nf] = txid
			}
		}

		// Replace the cache only after every transaction in one validated snapshot
		// has a complete decode and the snapshot has no nullifier collisions.
		s.mempoolOrchardNullifiersByTxID = nullifiersByTxID
		s.mempoolTxExpiryHeightByTxID = expiryHeightByTxID
		newTxHex = decodedHex
		newTxExpiryHeight = decodedExpiry
		decodedSnapshot = true
		break
	}
	if !decodedSnapshot {
		return errors.New("scanner: mempool changed repeatedly while decoding transactions")
	}

	pending := make(map[string]store.PendingSpend)
	for txid, nfs := range s.mempoolOrchardNullifiersByTxID {
		expPtr := s.mempoolTxExpiryHeightByTxID[txid]
		// Treat expired txs as non-pending even if the daemon briefly reports them in the mempool.
		if expPtr != nil && chainHeight > *expPtr {
			continue
		}
		for _, nf := range nfs {
			if nf == "" {
				continue
			}
			pending[nf] = store.PendingSpend{
				TxID:         txid,
				ExpiryHeight: expPtr,
			}
		}
	}

	if err := s.st.UpdatePendingSpends(ctx, pending, chainHeight, seenAt); err != nil {
		return fmt.Errorf("scanner: update pending spends: %w", err)
	}

	if len(newTxHex) > 0 {
		newTxIDs := make([]string, 0, len(newTxHex))
		for txid := range newTxHex {
			newTxIDs = append(newTxIDs, txid)
		}

		notes, err := s.st.ListNotesByPendingSpentTxIDs(ctx, newTxIDs)
		if err != nil {
			return fmt.Errorf("scanner: list pending spend notes: %w", err)
		}

		wallets, err := s.loadWallets(ctx)
		if err != nil {
			return err
		}
		walletByID := make(map[string]orchardscan.Wallet, len(wallets))
		for _, w := range wallets {
			walletByID[w.WalletID] = w
		}

		walletIDsByTxID := make(map[string]map[string]struct{})
		for _, n := range notes {
			if n.PendingSpentTxID == nil {
				continue
			}
			txid := strings.ToLower(strings.TrimSpace(*n.PendingSpentTxID))
			if _, ok := newTxHex[txid]; !ok {
				continue
			}
			if _, ok := walletIDsByTxID[txid]; !ok {
				walletIDsByTxID[txid] = make(map[string]struct{})
			}
			walletIDsByTxID[txid][n.WalletID] = struct{}{}
		}

		for txid, walletSet := range walletIDsByTxID {
			txHex := newTxHex[txid]
			if strings.TrimSpace(txHex) == "" || len(walletSet) == 0 {
				continue
			}
			expiryHeight := newTxExpiryHeight[txid]

			recoverWallets := make([]orchardscan.Wallet, 0, len(walletSet))
			for walletID := range walletSet {
				w, ok := walletByID[walletID]
				if !ok {
					continue
				}
				recoverWallets = append(recoverWallets, w)
			}
			if len(recoverWallets) == 0 {
				continue
			}

			outs, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, recoverWallets, txHex)
			if err != nil {
				log.Printf("mempool: recover outgoing tx %s failed: %v", txid, err)
				continue
			}

			if err := s.st.WithTx(ctx, func(tx store.Tx) error {
				for _, o := range outs {
					seenAtCopy := seenAt

					var memoHexPtr *string
					memoHex := strings.TrimSpace(o.MemoHex)
					if memoHex != "" {
						memoHexPtr = &memoHex
					}

					var recipientScopePtr *string
					recipientScope := strings.TrimSpace(o.RecipientScope)
					if recipientScope != "" {
						recipientScopePtr = &recipientScope
					}

					changed, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
						WalletID: o.WalletID,
						TxID:     txid,

						ActionIndex: int32(o.ActionIndex),

						MempoolSeenAt:  &seenAtCopy,
						TxExpiryHeight: expiryHeight,

						RecipientAddress: o.RecipientAddress,
						ValueZat:         int64(o.ValueZat),
						MemoHex:          memoHexPtr,

						OvkScope:       o.OvkScope,
						RecipientScope: recipientScopePtr,
					})
					if err != nil {
						return err
					}
					if !changed {
						continue
					}

					payload := events.OutgoingOutputEventPayload{
						Version:  types.V1,
						WalletID: o.WalletID,

						TxID:         txid,
						ExpiryHeight: expiryHeight,

						ActionIndex:    o.ActionIndex,
						AmountZatoshis: o.ValueZat,

						RecipientAddress: o.RecipientAddress,
						MemoHex:          memoHex,

						OvkScope:       o.OvkScope,
						RecipientScope: recipientScope,

						Status: types.TxStatus{
							State: types.TxStateMempool,
						},
					}
					payloadBytes, err := json.Marshal(payload)
					if err != nil {
						return fmt.Errorf("scanner: marshal outgoing output mempool payload: %w", err)
					}
					if err := tx.InsertEvent(ctx, store.Event{
						Kind:     events.KindOutgoingOutputEvent,
						WalletID: o.WalletID,
						Height:   0,
						Payload:  payloadBytes,
					}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}

	// Deterministic expiry: we only emit an event once chainHeight has passed tx_expiry_height.
	if err := s.st.WithTx(ctx, func(tx store.Tx) error {
		expired, err := tx.ExpireOutgoingOutputs(ctx, chainHeight, seenAt)
		if err != nil {
			return err
		}
		for _, o := range expired {
			memoHex := ""
			if o.MemoHex != nil {
				memoHex = strings.TrimSpace(*o.MemoHex)
			}

			payload := events.OutgoingOutputEventPayload{
				Version:  types.V1,
				WalletID: o.WalletID,

				TxID:         o.TxID,
				ExpiryHeight: o.TxExpiryHeight,

				ActionIndex:    uint32(o.ActionIndex),
				AmountZatoshis: uint64(o.ValueZat),

				RecipientAddress: o.RecipientAddress,
				MemoHex:          memoHex,

				OvkScope:       o.OvkScope,
				RecipientScope: derefString(o.RecipientScope),

				Status: types.TxStatus{
					State: types.TxStateExpired,
				},
			}
			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("scanner: marshal outgoing output expired payload: %w", err)
			}
			if err := tx.InsertEvent(ctx, store.Event{
				Kind:     events.KindOutgoingOutputExpired,
				WalletID: o.WalletID,
				Height:   chainHeight,
				Payload:  payloadBytes,
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	eventEpoch, err := s.st.EventEpoch(ctx)
	if err != nil {
		return fmt.Errorf("scanner: read event epoch after mempool refresh: %w", err)
	}
	tip, ok, err := s.st.Tip(ctx)
	if err != nil {
		return fmt.Errorf("scanner: read tip after mempool refresh: %w", err)
	}
	if !ok || tip.Height != chainHeight {
		return fmt.Errorf("scanner: mempool refresh tip mismatch: found=%t height=%d chain_height=%d", ok, tip.Height, chainHeight)
	}
	s.recordMempoolRefresh(eventEpoch, tip)
	refreshSucceeded = true

	return nil
}

func validateMempoolTxIDs(txids []string) ([]string, map[string]struct{}, error) {
	ordered := make([]string, 0, len(txids))
	unique := make(map[string]struct{}, len(txids))
	for _, txid := range txids {
		if !isCanonicalLowerHex64(txid) {
			return nil, nil, fmt.Errorf("scanner: getrawmempool returned non-canonical transaction id")
		}
		if _, duplicate := unique[txid]; duplicate {
			return nil, nil, fmt.Errorf("scanner: getrawmempool returned duplicate transaction id %s", txid)
		}
		unique[txid] = struct{}{}
		ordered = append(ordered, txid)
	}
	return ordered, unique, nil
}

func isCanonicalLowerHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := range value {
		if (value[i] < '0' || value[i] > '9') && (value[i] < 'a' || value[i] > 'f') {
			return false
		}
	}
	return true
}

func (s *Scanner) findCommonAncestor(ctx context.Context, fromHeight int64) (int64, error) {
	h := fromHeight
	for h >= 0 {
		dbHash, ok, err := s.st.HashAtHeight(ctx, h)
		if err != nil {
			return 0, err
		}
		if !ok {
			h--
			continue
		}
		chainHash, err := s.rpc.GetBlockHash(ctx, h)
		if err != nil {
			return 0, fmt.Errorf("scanner: getblockhash(%d) while finding common ancestor: %w", h, err)
		}
		if chainHash == dbHash {
			return h, nil
		}
		h--
	}
	return -1, nil
}

func (s *Scanner) processBlock(ctx context.Context, blk blockVerbose2) error {
	wallets, err := s.loadWallets(ctx)
	if err != nil {
		return err
	}

	walletByID := make(map[string]orchardscan.Wallet, len(wallets))
	for _, w := range wallets {
		walletByID[w.WalletID] = w
	}

	if err := s.st.WithTx(ctx, func(tx store.Tx) error {
		if err := tx.InsertBlock(ctx, store.Block{
			Height:   blk.Height,
			Hash:     blk.Hash,
			PrevHash: blk.PreviousBlockHash,
			Time:     blk.Time,
		}); err != nil {
			return err
		}

		nextPos, err := tx.NextOrchardCommitmentPosition(ctx)
		if err != nil {
			return err
		}
		subtreeTx, supportsSubtreeRoots := tx.(orchardSubtreeRootTx)

		posByTxAction := make(map[string]map[uint32]int64)
		txHexes := make([]string, 0, len(blk.Tx))
		for _, t := range blk.Tx {
			txHexes = append(txHexes, t.Hex)
		}
		scanResults, err := orchardscan.ScanBlock(ctx, s.uaHRP, wallets, txHexes)
		if err != nil {
			return fmt.Errorf("scanner: scan block %d: %w", blk.Height, err)
		}

		for txIndex, t := range blk.Tx {
			res := scanResults[txIndex]

			if len(res.Actions) == 0 {
				continue
			}

			if _, ok := posByTxAction[t.TxID]; !ok {
				posByTxAction[t.TxID] = make(map[uint32]int64, len(res.Actions))
			}

			for _, a := range res.Actions {
				if err := tx.InsertOrchardAction(ctx, store.OrchardAction{
					Height:          blk.Height,
					TxID:            t.TxID,
					ActionIndex:     int32(a.ActionIndex),
					ActionNullifier: a.ActionNullifier,
					CMX:             a.CMX,
					EphemeralKey:    a.EphemeralKey,
					EncCiphertext:   a.EncCiphertext,
				}); err != nil {
					return err
				}

				pos := nextPos
				nextPos++
				posByTxAction[t.TxID][a.ActionIndex] = pos

				if err := tx.InsertOrchardCommitment(ctx, store.OrchardCommitment{
					Position:    pos,
					Height:      blk.Height,
					TxID:        t.TxID,
					ActionIndex: int32(a.ActionIndex),
					CMX:         a.CMX,
				}); err != nil {
					return err
				}
				if supportsSubtreeRoots && nextPos > 0 && nextPos%orchardSubtreeLeafCount == 0 {
					if err := persistCompletedSubtreeRoot(ctx, subtreeTx, blk, nextPos); err != nil {
						return err
					}
				}
			}

			// Mark spends.
			nullifiers := make([]string, 0, len(res.Actions))
			for _, a := range res.Actions {
				nullifiers = append(nullifiers, a.ActionNullifier)
			}
			spentNotes, err := tx.MarkNotesSpent(ctx, blk.Height, t.TxID, nullifiers)
			if err != nil {
				return err
			}
			for _, n := range spentNotes {
				payload := events.SpendEventPayload{
					Version:          types.V1,
					WalletID:         n.WalletID,
					DiversifierIndex: n.DiversifierIndex,
					TxID:             t.TxID,
					Height:           blk.Height,
					NoteTxID:         n.TxID,
					NoteActionIndex:  uint32(n.ActionIndex),
					NoteHeight:       n.Height,
					AmountZatoshis:   uint64(n.ValueZat),
					NoteNullifier:    n.NoteNullifier,
					RecipientAddress: n.RecipientAddress,
					Status: types.TxStatus{
						State:         types.TxStateConfirmed,
						Height:        blk.Height,
						Confirmations: 1,
					},
				}
				payloadBytes, err := json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("scanner: marshal spend payload: %w", err)
				}
				if err := tx.InsertEvent(ctx, store.Event{
					Kind:     events.KindSpendEvent,
					WalletID: n.WalletID,
					Height:   blk.Height,
					Payload:  payloadBytes,
				}); err != nil {
					return err
				}
			}

			recoveredByRegisteredWallet, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, wallets, t.Hex)
			if err != nil {
				return fmt.Errorf("scanner: classify transaction origin %s: %w", t.TxID, err)
			}
			isInternalTx := len(spentNotes) > 0 || len(recoveredByRegisteredWallet) > 0
			if isInternalTx {
				if _, err := tx.MarkTransactionInternal(ctx, t.TxID); err != nil {
					return err
				}
			}

			// Record outgoing outputs for spends (recipient/value/memo via OVK output recovery).
			if len(spentNotes) > 0 {
				walletsSet := make(map[string]orchardscan.Wallet)
				for _, n := range spentNotes {
					w, ok := walletByID[n.WalletID]
					if !ok {
						continue
					}
					walletsSet[n.WalletID] = w
				}

				if len(walletsSet) > 0 {
					recoverWallets := make([]orchardscan.Wallet, 0, len(walletsSet))
					for _, w := range walletsSet {
						recoverWallets = append(recoverWallets, w)
					}

					outs, err := orchardscan.RecoverOutgoingTx(ctx, s.uaHRP, recoverWallets, t.Hex)
					if err != nil {
						return fmt.Errorf("scanner: recover outgoing tx %s: %w", t.TxID, err)
					}

					for _, o := range outs {
						heightCopy := blk.Height
						pos := posByTxAction[t.TxID][o.ActionIndex]
						posCopy := pos

						var memoHexPtr *string
						memoHex := strings.TrimSpace(o.MemoHex)
						if memoHex != "" {
							memoHexPtr = &memoHex
						}

						var recipientScopePtr *string
						recipientScope := strings.TrimSpace(o.RecipientScope)
						if recipientScope != "" {
							recipientScopePtr = &recipientScope
						}

						changed, err := tx.InsertOutgoingOutput(ctx, store.OutgoingOutput{
							WalletID: o.WalletID,
							TxID:     t.TxID,

							ActionIndex: int32(o.ActionIndex),

							MinedHeight: &heightCopy,
							Position:    &posCopy,

							RecipientAddress: o.RecipientAddress,
							ValueZat:         int64(o.ValueZat),
							MemoHex:          memoHexPtr,

							OvkScope:       o.OvkScope,
							RecipientScope: recipientScopePtr,
						})
						if err != nil {
							return err
						}
						if !changed {
							continue
						}

						payload := events.OutgoingOutputEventPayload{
							Version:  types.V1,
							WalletID: o.WalletID,

							TxID:   t.TxID,
							Height: &heightCopy,

							ActionIndex:    o.ActionIndex,
							AmountZatoshis: o.ValueZat,

							RecipientAddress: o.RecipientAddress,
							MemoHex:          memoHex,

							OvkScope:       o.OvkScope,
							RecipientScope: recipientScope,

							Status: types.TxStatus{
								State:         types.TxStateConfirmed,
								Height:        blk.Height,
								Confirmations: 1,
							},
						}
						payloadBytes, err := json.Marshal(payload)
						if err != nil {
							return fmt.Errorf("scanner: marshal outgoing output payload: %w", err)
						}
						if err := tx.InsertEvent(ctx, store.Event{
							Kind:     events.KindOutgoingOutputEvent,
							WalletID: o.WalletID,
							Height:   blk.Height,
							Payload:  payloadBytes,
						}); err != nil {
							return err
						}
					}
				}
			}

			// Record deposits.
			for _, n := range res.Notes {
				pos := posByTxAction[t.TxID][n.ActionIndex]
				posCopy := pos
				var memoHexPtr *string
				if n.MemoHex != "" {
					memo := n.MemoHex
					memoHexPtr = &memo
				}
				inserted, err := tx.InsertNote(ctx, store.Note{
					WalletID:         n.WalletID,
					TxID:             t.TxID,
					ActionIndex:      int32(n.ActionIndex),
					Height:           blk.Height,
					Position:         &posCopy,
					IsInternal:       isInternalTx,
					DiversifierIndex: n.DiversifierIndex,
					RecipientAddress: n.RecipientAddress,
					ValueZat:         int64(n.ValueZat),
					MemoHex:          memoHexPtr,
					NoteNullifier:    n.NoteNullifier,
				})
				if err != nil {
					return err
				}

				if inserted && !isInternalTx {
					payload := events.DepositEventPayload{
						Origin: string(types.DepositOriginExternal),
						DepositEvent: events.DepositEvent{
							Version:          types.V1,
							WalletID:         n.WalletID,
							DiversifierIndex: n.DiversifierIndex,
							TxID:             t.TxID,
							Height:           blk.Height,
							ActionIndex:      n.ActionIndex,
							AmountZatoshis:   n.ValueZat,
							MemoHex:          n.MemoHex,
							Status: types.TxStatus{
								State:         types.TxStateConfirmed,
								Height:        blk.Height,
								Confirmations: 1,
							},
						},
						RecipientAddress: n.RecipientAddress,
						NoteNullifier:    n.NoteNullifier,
					}
					payloadBytes, err := json.Marshal(payload)
					if err != nil {
						return fmt.Errorf("scanner: marshal event payload: %w", err)
					}
					if err := tx.InsertEvent(ctx, store.Event{
						Kind:     events.KindDepositEvent,
						WalletID: n.WalletID,
						Height:   blk.Height,
						Payload:  payloadBytes,
					}); err != nil {
						return err
					}
				}
			}
		}

		if err := s.confirmDepositConfirmations(ctx, tx, blk.Height); err != nil {
			return err
		}
		if err := s.confirmSpendConfirmations(ctx, tx, blk.Height); err != nil {
			return err
		}
		if err := s.confirmOutgoingOutputConfirmations(ctx, tx, blk.Height); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// Reconcile once more after the block transaction is visible. Together with
	// the API's post-CAS tip check, this closes both orderings of a backfill
	// completion crossing the live block commit.
	return reconcileCompleteWalletBackfillProgress(ctx, s.st, blk.Height)
}

func reconcileCompleteWalletBackfillProgress(ctx context.Context, st store.Store, scannedHeight int64) error {
	return st.WithTx(ctx, func(tx store.Tx) error {
		return tx.AdvanceCompleteWalletBackfillProgress(ctx, scannedHeight)
	})
}

func persistCompletedSubtreeRoot(ctx context.Context, tx orchardSubtreeRootTx, blk blockVerbose2, nextPos int64) error {
	subtreeIndex := (nextPos / orchardSubtreeLeafCount) - 1
	if subtreeIndex < 0 {
		return errors.New("scanner: negative subtree index")
	}
	startPos := subtreeIndex * orchardSubtreeLeafCount
	endPos := startPos + orchardSubtreeLeafCount

	cmxHex, err := tx.ListOrchardCommitmentCMXByPositionRange(ctx, startPos, endPos)
	if err != nil {
		return fmt.Errorf("scanner: list subtree cmx range: %w", err)
	}
	if int64(len(cmxHex)) != orchardSubtreeLeafCount {
		return fmt.Errorf("scanner: incomplete subtree commitments index=%d count=%d", subtreeIndex, len(cmxHex))
	}

	rootHex, err := orchardscan.OrchardSubtreeRoot(ctx, cmxHex)
	if err != nil {
		return fmt.Errorf("scanner: compute subtree root: %w", err)
	}

	if err := tx.InsertOrchardSubtreeRoot(ctx, store.OrchardSubtreeRoot{
		SubtreeIndex: subtreeIndex,
		EndPosition:  endPos - 1,
		EndHeight:    blk.Height,
		EndBlockHash: blk.Hash,
		Root:         rootHex,
	}); err != nil {
		return fmt.Errorf("scanner: insert subtree root: %w", err)
	}
	return nil
}

func (s *Scanner) backfillMissingSubtreeRoots(ctx context.Context, anchorHeight int64, maxSubtrees int) error {
	if maxSubtrees <= 0 {
		return nil
	}
	backfillStore, ok := s.st.(orchardSubtreeRootBackfillStore)
	if !ok {
		return nil
	}

	leafCount, err := backfillStore.OrchardTreeSizeAtHeight(ctx, anchorHeight)
	if err != nil {
		return fmt.Errorf("scanner: subtree backfill tree size: %w", err)
	}
	fullSubtrees := leafCount / orchardSubtreeLeafCount
	if fullSubtrees <= 0 {
		return nil
	}

	rootExists := make(map[int64]struct{})
	for start := int64(0); start < fullSubtrees; start += orchardSubtreeRootChunkSize {
		limit := int(fullSubtrees - start)
		if limit > orchardSubtreeRootChunkSize {
			limit = orchardSubtreeRootChunkSize
		}
		roots, err := backfillStore.ListOrchardSubtreeRootsByIndexRange(ctx, start, limit)
		if err != nil {
			return fmt.Errorf("scanner: subtree backfill list roots: %w", err)
		}
		for _, r := range roots {
			if r.SubtreeIndex < 0 || r.SubtreeIndex >= fullSubtrees {
				continue
			}
			rootExists[r.SubtreeIndex] = struct{}{}
		}
	}

	inserted := 0
	for subtreeIndex := int64(0); subtreeIndex < fullSubtrees && inserted < maxSubtrees; subtreeIndex++ {
		if _, ok := rootExists[subtreeIndex]; ok {
			continue
		}

		startPos := subtreeIndex * orchardSubtreeLeafCount
		endPos := startPos + orchardSubtreeLeafCount

		cmxHex, err := backfillStore.ListOrchardCommitmentCMXByPositionRange(ctx, anchorHeight, startPos, endPos)
		if err != nil {
			return fmt.Errorf("scanner: subtree backfill list cmx: %w", err)
		}
		if int64(len(cmxHex)) != orchardSubtreeLeafCount {
			return fmt.Errorf("scanner: subtree backfill incomplete commitments index=%d count=%d", subtreeIndex, len(cmxHex))
		}

		rootHex, err := orchardscan.OrchardSubtreeRoot(ctx, cmxHex)
		if err != nil {
			return fmt.Errorf("scanner: subtree backfill compute root: %w", err)
		}

		endCommitment, err := backfillStore.ListOrchardCommitmentsByPositionsUpToHeight(ctx, anchorHeight, []int64{endPos - 1})
		if err != nil {
			return fmt.Errorf("scanner: subtree backfill lookup end commitment: %w", err)
		}
		if len(endCommitment) != 1 {
			return fmt.Errorf("scanner: subtree backfill missing end commitment index=%d", subtreeIndex)
		}
		endHeight := endCommitment[0].Height

		endBlockHash, ok, err := s.st.HashAtHeight(ctx, endHeight)
		if err != nil {
			return fmt.Errorf("scanner: subtree backfill lookup end block hash: %w", err)
		}
		if !ok {
			return fmt.Errorf("scanner: subtree backfill missing end block hash height=%d", endHeight)
		}

		if err := s.st.WithTx(ctx, func(tx store.Tx) error {
			subtreeTx, ok := tx.(orchardSubtreeRootTx)
			if !ok {
				return nil
			}
			return subtreeTx.InsertOrchardSubtreeRoot(ctx, store.OrchardSubtreeRoot{
				SubtreeIndex: subtreeIndex,
				EndPosition:  endPos - 1,
				EndHeight:    endHeight,
				EndBlockHash: endBlockHash,
				Root:         rootHex,
			})
		}); err != nil {
			return fmt.Errorf("scanner: subtree backfill insert root: %w", err)
		}
		inserted++
	}

	return nil
}

func (s *Scanner) confirmDepositConfirmations(ctx context.Context, tx store.Tx, scanHeight int64) error {
	maxNoteHeight := scanHeight - s.confirmations + 1
	if maxNoteHeight < 0 {
		return nil
	}

	notes, err := tx.ConfirmNotes(ctx, scanHeight, maxNoteHeight)
	if err != nil {
		return err
	}

	for _, n := range notes {
		if n.IsInternal {
			continue
		}
		memoHex := ""
		if n.MemoHex != nil {
			memoHex = *n.MemoHex
		}
		confirmations := scanHeight - n.Height + 1
		if confirmations < 1 {
			confirmations = 1
		}

		payload := events.DepositConfirmedPayload{
			DepositEventPayload: events.DepositEventPayload{
				Origin: string(types.DepositOriginExternal),
				DepositEvent: events.DepositEvent{
					Version:          types.V1,
					WalletID:         n.WalletID,
					DiversifierIndex: n.DiversifierIndex,
					TxID:             n.TxID,
					Height:           n.Height,
					ActionIndex:      uint32(n.ActionIndex),
					AmountZatoshis:   uint64(n.ValueZat),
					MemoHex:          memoHex,
					Status: types.TxStatus{
						State:         types.TxStateConfirmed,
						Height:        n.Height,
						Confirmations: confirmations,
					},
				},
				RecipientAddress: n.RecipientAddress,
				NoteNullifier:    n.NoteNullifier,
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindDepositConfirmed,
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scanner) confirmSpendConfirmations(ctx context.Context, tx store.Tx, scanHeight int64) error {
	maxSpentHeight := scanHeight - s.confirmations + 1
	if maxSpentHeight < 0 {
		return nil
	}

	notes, err := tx.ConfirmSpends(ctx, scanHeight, maxSpentHeight)
	if err != nil {
		return err
	}

	for _, n := range notes {
		if n.SpentHeight == nil || n.SpentTxID == nil {
			continue
		}
		confirmations := scanHeight - *n.SpentHeight + 1
		if confirmations < 1 {
			confirmations = 1
		}

		payload := events.SpendConfirmedPayload{
			SpendEventPayload: events.SpendEventPayload{
				Version:          types.V1,
				WalletID:         n.WalletID,
				DiversifierIndex: n.DiversifierIndex,
				TxID:             *n.SpentTxID,
				Height:           *n.SpentHeight,
				NoteTxID:         n.TxID,
				NoteActionIndex:  uint32(n.ActionIndex),
				NoteHeight:       n.Height,
				AmountZatoshis:   uint64(n.ValueZat),
				NoteNullifier:    n.NoteNullifier,
				RecipientAddress: n.RecipientAddress,
				Status: types.TxStatus{
					State:         types.TxStateConfirmed,
					Height:        *n.SpentHeight,
					Confirmations: confirmations,
				},
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal spend confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindSpendConfirmed,
			WalletID: n.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scanner) confirmOutgoingOutputConfirmations(ctx context.Context, tx store.Tx, scanHeight int64) error {
	maxMinedHeight := scanHeight - s.confirmations + 1
	if maxMinedHeight < 0 {
		return nil
	}

	outs, err := tx.ConfirmOutgoingOutputs(ctx, scanHeight, maxMinedHeight)
	if err != nil {
		return err
	}

	for _, o := range outs {
		if o.MinedHeight == nil {
			continue
		}

		confCount := scanHeight - *o.MinedHeight + 1
		if confCount < 1 {
			confCount = 1
		}

		memoHex := ""
		if o.MemoHex != nil {
			memoHex = *o.MemoHex
		}

		heightCopy := *o.MinedHeight
		payload := events.OutgoingOutputConfirmedPayload{
			OutgoingOutputEventPayload: events.OutgoingOutputEventPayload{
				Version:  types.V1,
				WalletID: o.WalletID,

				TxID:   o.TxID,
				Height: &heightCopy,

				ActionIndex:    uint32(o.ActionIndex),
				AmountZatoshis: uint64(o.ValueZat),

				RecipientAddress: o.RecipientAddress,
				MemoHex:          memoHex,

				OvkScope:       o.OvkScope,
				RecipientScope: derefString(o.RecipientScope),

				Status: types.TxStatus{
					State:         types.TxStateConfirmed,
					Height:        *o.MinedHeight,
					Confirmations: confCount,
				},
			},
			ConfirmedHeight:       scanHeight,
			RequiredConfirmations: s.confirmations,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("scanner: marshal outgoing output confirmed payload: %w", err)
		}
		if err := tx.InsertEvent(ctx, store.Event{
			Kind:     events.KindOutgoingOutputConfirmed,
			WalletID: o.WalletID,
			Height:   scanHeight,
			Payload:  payloadBytes,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scanner) loadWallets(ctx context.Context) ([]orchardscan.Wallet, error) {
	wallets, err := s.st.ListEnabledWalletUFVKs(ctx)
	if err != nil {
		return nil, fmt.Errorf("scanner: list wallets: %w", err)
	}
	out := make([]orchardscan.Wallet, 0, len(wallets))
	for _, w := range wallets {
		out = append(out, orchardscan.Wallet{
			WalletID: w.WalletID,
			UFVK:     w.UFVK,
		})
	}
	return out, nil
}

type blockVerbose2 struct {
	Hash              string       `json:"hash"`
	Height            int64        `json:"height"`
	Time              int64        `json:"time"`
	PreviousBlockHash string       `json:"previousblockhash,omitempty"`
	Tx                []txVerbose2 `json:"tx"`
}

type txVerbose2 struct {
	TxID string `json:"txid"`
	Hex  string `json:"hex"`
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
