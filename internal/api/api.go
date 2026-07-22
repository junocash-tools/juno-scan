package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/backfill"
	"github.com/Abdullah1738/juno-scan/internal/events"
	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/shardcache"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

type noteDirection string

const (
	noteDirectionIncoming noteDirection = "incoming"
	noteDirectionOutgoing noteDirection = "outgoing"
	noteDirectionAll      noteDirection = "all"
)

type apiWalletNote struct {
	Direction           noteDirection `json:"direction"`
	TxID                string        `json:"txid"`
	ActionIndex         int32         `json:"action_index"`
	Height              int64         `json:"height"`
	Position            *int64        `json:"position,omitempty"`
	Recipient           string        `json:"recipient_address"`
	ValueZat            int64         `json:"value_zat"`
	MemoHex             *string       `json:"memo_hex"`
	NoteNullifier       *string       `json:"note_nullifier"`
	OvkScope            *string       `json:"ovk_scope,omitempty"`
	RecipientScope      *string       `json:"recipient_scope,omitempty"`
	PendingTxID         *string       `json:"pending_spent_txid,omitempty"`
	PendingAt           *time.Time    `json:"pending_spent_at,omitempty"`
	PendingExpiryHeight *int64        `json:"pending_spent_expiry_height,omitempty"`
	SpentHeight         *int64        `json:"spent_height,omitempty"`
	SpentTxID           *string       `json:"spent_txid,omitempty"`
	CreatedAt           time.Time     `json:"created_at"`
}

type Server struct {
	st store.Store
	bf *backfill.Service

	bearerToken          string
	defaultConfirmations int64
	network              string
	uaHRP                string
	nodeHeight           func() (int64, bool)
	maxReadyLag          int64
	shardCache           *shardcache.Service
	witnessMode          string
	mempoolRefresh       func() MempoolRefreshStatus
}

type MempoolRefreshStatus struct {
	Ready      bool
	EventEpoch string
	Height     int64
	Hash       string
}

type Option func(*Server)

func WithBackfillService(bf *backfill.Service) Option {
	return func(s *Server) {
		s.bf = bf
	}
}

func WithBearerToken(token string) Option {
	token = strings.TrimSpace(token)
	return func(s *Server) {
		s.bearerToken = token
	}
}

func WithRuntimeStatus(network, uaHRP string, defaultConfirmations, maxReadyLag int64, nodeHeight func() (int64, bool)) Option {
	return func(s *Server) {
		s.network = strings.TrimSpace(network)
		s.uaHRP = strings.TrimSpace(uaHRP)
		if defaultConfirmations > 0 {
			s.defaultConfirmations = defaultConfirmations
		}
		if maxReadyLag >= 0 {
			s.maxReadyLag = maxReadyLag
		}
		s.nodeHeight = nodeHeight
	}
}

func WithShardCacheService(cache *shardcache.Service) Option {
	return func(s *Server) { s.shardCache = cache }
}

func WithWitnessMode(mode string) Option {
	return func(s *Server) {
		mode = strings.ToLower(strings.TrimSpace(mode))
		if mode == "" {
			mode = "auto"
		}
		s.witnessMode = mode
	}
}

func WithMempoolRefreshStatus(status func() MempoolRefreshStatus) Option {
	return func(s *Server) { s.mempoolRefresh = status }
}

func New(st store.Store, opts ...Option) (*Server, error) {
	if st == nil {
		return nil, errors.New("api: store is nil")
	}
	s := &Server{st: st, defaultConfirmations: 100, maxReadyLag: 2, witnessMode: "auto"}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	switch s.witnessMode {
	case "auto", "shard", "subtree", "legacy":
	default:
		return nil, errors.New("api: invalid witness mode")
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/orchard/witness", s.handleOrchardWitness)
	mux.HandleFunc("/v1/wallets", s.handleWallets)
	mux.HandleFunc("/v1/wallets/", s.handleWalletSubroutes)

	if s.bearerToken == "" {
		return mux
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validBearerToken(r, s.bearerToken) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp := map[string]any{
		"status":        "ok",
		"ready":         true,
		"confirmations": s.defaultConfirmations,
		"max_ready_lag": s.maxReadyLag,
	}
	eventEpoch, err := s.st.EventEpoch(ctx)
	if err != nil || len(eventEpoch) != store.EventEpochHexLength || !isLowerHex(eventEpoch) {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	resp["event_epoch"] = eventEpoch
	refresh := s.mempoolRefreshStatus()
	resp["pending_spends_ready"] = false
	if s.network != "" {
		resp["network"] = s.network
	}
	if s.uaHRP != "" {
		resp["ua_hrp"] = s.uaHRP
	}
	if s.shardCache != nil {
		resp["shard_cache"] = s.shardCache.Snapshot()
	}
	if s.bf != nil {
		resp["backfills"] = s.bf.Snapshot()
	}

	if tip, ok, err := s.st.Tip(ctx); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	} else if ok {
		resp["scanned_height"] = tip.Height
		resp["scanned_hash"] = tip.Hash
		actionIndex := map[string]any{"indexed_through": tip.Height}
		if counter, ok := s.st.(interface {
			CountOrchardActionHeights(context.Context) (int64, error)
		}); ok {
			if count, countErr := counter.CountOrchardActionHeights(ctx); countErr == nil {
				actionIndex["action_heights"] = count
			}
		}
		resp["action_index"] = actionIndex
		mempoolRefreshCurrent := refreshMatches(refresh, eventEpoch, tip)
		resp["pending_spends_ready"] = mempoolRefreshCurrent
		if !mempoolRefreshCurrent {
			resp["ready"] = false
			resp["status"] = "degraded"
		}
		historyComplete := true
		historyPending := 0
		wallets, walletsErr := s.st.ListWallets(ctx)
		if walletsErr != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		progressStore, hasProgress := s.st.(walletBackfillStore)
		for _, wallet := range wallets {
			if wallet.DisabledAt != nil {
				continue
			}
			if !hasProgress {
				historyComplete = false
				historyPending++
				continue
			}
			progress, found, progressErr := progressStore.WalletBackfillStatus(ctx, wallet.WalletID)
			if progressErr != nil {
				http.Error(w, "db error", http.StatusInternalServerError)
				return
			}
			if !found || progress.State != "complete" || progress.Generation < 1 || progress.BirthdayHeight != wallet.BirthdayHeight || progress.UFVKFingerprint != wallet.UFVKFingerprint || progress.NextHeight <= tip.Height || progress.NextHeight < progress.BirthdayHeight {
				historyComplete = false
				historyPending++
			}
		}
		resp["history_complete"] = historyComplete
		resp["history_pending_wallets"] = historyPending
		if !historyComplete {
			resp["ready"] = false
			resp["status"] = "degraded"
		}
		if s.nodeHeight != nil {
			if nodeHeight, known := s.nodeHeight(); known {
				lag := nodeHeight - tip.Height
				scannerAhead := lag < 0
				if lag < 0 {
					lag = 0
				}
				resp["node_height"] = nodeHeight
				resp["scanner_lag"] = lag
				if scannerAhead || lag > s.maxReadyLag {
					resp["ready"] = false
					resp["status"] = "degraded"
				}
			} else {
				resp["ready"] = false
				resp["status"] = "degraded"
			}
		}
	} else {
		resp["ready"] = false
		resp["status"] = "degraded"
	}

	writeJSON(w, resp)
}

func (s *Server) handleWallets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListWallets(w, r)
	case http.MethodPost:
		s.handleUpsertWallet(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWalletSubroutes(w http.ResponseWriter, r *http.Request) {
	// /v1/wallets/{wallet_id}/events
	path := strings.TrimPrefix(r.URL.Path, "/v1/wallets/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	walletID := parts[0]
	switch parts[1] {
	case "events":
		s.handleListWalletEvents(w, r, walletID)
	case "notes":
		switch {
		case len(parts) == 2:
			s.handleListWalletNotes(w, r, walletID)
		case len(parts) == 3 && parts[2] == "summary":
			s.handleWalletNoteSummary(w, r, walletID)
		case len(parts) == 3 && parts[2] == "status":
			s.handleWalletNoteStatuses(w, r, walletID)
		default:
			http.NotFound(w, r)
		}
	case "backfill":
		s.handleBackfillWallet(w, r, walletID)
	case "addresses":
		if len(parts) == 4 && parts[2] != "" && parts[3] == "balance" {
			s.handleAddressBalance(w, r, walletID, parts[2])
			return
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

type apiWalletNoteSummary struct {
	WalletID           string                        `json:"wallet_id"`
	MinConfirmations   int64                         `json:"min_confirmations"`
	MinNoteZat         int64                         `json:"min_note_zat"`
	AsOfScannerHeight  int64                         `json:"as_of_scanner_height"`
	AsOfScannerHash    string                        `json:"as_of_scanner_hash"`
	TotalUnspent       store.NoteValueSummary        `json:"total_unspent"`
	Spendable          store.SpendableNoteSummary    `json:"spendable"`
	Immature           store.NoteValueSummary        `json:"immature"`
	PendingSpend       store.PendingSpendNoteSummary `json:"pending_spend"`
	BelowMinNote       store.NoteValueSummary        `json:"below_min_note"`
	WitnessUnavailable store.NoteValueSummary        `json:"witness_unavailable"`
}

const walletNoteStatusMaxBodyBytes = 32 << 10

var errWalletNoteStatusSnapshotChanged = errors.New("wallet note status snapshot changed")
var errWalletNoteStatusMempoolNotReady = errors.New("wallet note status mempool refresh is not current")

type apiWalletNoteStatusRequest struct {
	NoteIDs []string `json:"note_ids"`
}

type apiWalletNoteStatus struct {
	NoteID                   string     `json:"note_id"`
	State                    string     `json:"state"`
	SourceHeight             *int64     `json:"source_height,omitempty"`
	ValueZat                 *int64     `json:"value_zat,omitempty"`
	PendingSpentTxID         *string    `json:"pending_spent_txid,omitempty"`
	PendingSpentAt           *time.Time `json:"pending_spent_at,omitempty"`
	PendingSpentExpiryHeight *int64     `json:"pending_spent_expiry_height,omitempty"`
	SpentTxID                *string    `json:"spent_txid,omitempty"`
	SpentHeight              *int64     `json:"spent_height,omitempty"`
	SpentConfirmedHeight     *int64     `json:"spent_confirmed_height,omitempty"`
}

type apiWalletNoteStatusesResponse struct {
	WalletID          string                `json:"wallet_id"`
	EventEpoch        string                `json:"event_epoch"`
	AsOfScannerHeight int64                 `json:"as_of_scanner_height"`
	AsOfScannerHash   string                `json:"as_of_scanner_hash"`
	Statuses          []apiWalletNoteStatus `json:"statuses"`
}

type parsedWalletNoteStatusID struct {
	ID  string
	Ref store.NoteRef
}

func (s *Server) handleWalletNoteSummary(w http.ResponseWriter, r *http.Request, walletID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}

	minConfirmations := s.defaultConfirmations
	if raw := strings.TrimSpace(r.URL.Query().Get("min_confirmations")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 || value > 1_000_000 {
			http.Error(w, "invalid min_confirmations", http.StatusBadRequest)
			return
		}
		minConfirmations = value
	}
	minNoteZat := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("min_note_zat")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			http.Error(w, "invalid min_note_zat", http.StatusBadRequest)
			return
		}
		minNoteZat = value
	}
	maxNotes := int64(100_000)
	if raw := strings.TrimSpace(r.URL.Query().Get("max_notes")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 || value > 1_000_000 {
			http.Error(w, "invalid max_notes", http.StatusBadRequest)
			return
		}
		maxNotes = value
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	summary, err := s.st.WalletNoteSummary(ctx, walletID, minConfirmations, minNoteZat, int(maxNotes))
	if errors.Is(err, store.ErrWalletNoteSummaryLimit) {
		http.Error(w, "wallet note inventory exceeds max_notes", http.StatusUnprocessableEntity)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if !summary.WalletFound {
		http.Error(w, "wallet not found", http.StatusNotFound)
		return
	}
	if !summary.TipFound {
		http.Error(w, "scanner not ready", http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, apiWalletNoteSummary{
		WalletID:           walletID,
		MinConfirmations:   minConfirmations,
		MinNoteZat:         minNoteZat,
		AsOfScannerHeight:  summary.AsOfScannerHeight,
		AsOfScannerHash:    summary.AsOfScannerHash,
		TotalUnspent:       summary.TotalUnspent,
		Spendable:          summary.Spendable,
		Immature:           summary.Immature,
		PendingSpend:       summary.PendingSpend,
		BelowMinNote:       summary.BelowMinNote,
		WitnessUnavailable: summary.WitnessUnavailable,
	})
}

func (s *Server) handleWalletNoteStatuses(w http.ResponseWriter, r *http.Request, walletID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}

	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(r.Header.Get("Content-Type")))
	if err != nil || mediaType != "application/json" {
		http.Error(w, "content-type must be application/json", http.StatusBadRequest)
		return
	}
	if r.ContentLength > walletNoteStatusMaxBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, walletNoteStatusMaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request apiWalletNoteStatusRequest
	if err := decoder.Decode(&request); err != nil {
		writeWalletNoteStatusDecodeError(w, err)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeWalletNoteStatusDecodeError(w, err)
		return
	}
	if len(request.NoteIDs) < 1 || len(request.NoteIDs) > store.MaxNoteStatusBatch {
		http.Error(w, "note_ids must contain 1 to 200 items", http.StatusBadRequest)
		return
	}

	parsed := make([]parsedWalletNoteStatusID, 0, len(request.NoteIDs))
	refs := make([]store.NoteRef, 0, len(request.NoteIDs))
	seen := make(map[store.NoteRef]struct{}, len(request.NoteIDs))
	for _, noteID := range request.NoteIDs {
		ref, ok := parseSourceNoteID(noteID)
		if !ok {
			http.Error(w, "invalid note_id", http.StatusBadRequest)
			return
		}
		if _, duplicate := seen[ref]; duplicate {
			http.Error(w, "duplicate note_id", http.StatusBadRequest)
			return
		}
		seen[ref] = struct{}{}
		parsed = append(parsed, parsedWalletNoteStatusID{ID: noteID, Ref: ref})
		refs = append(refs, ref)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	snapshot, err := s.readStableWalletNoteStatuses(ctx, walletID, refs)
	if err != nil {
		switch {
		case errors.Is(err, errWalletNoteStatusSnapshotChanged):
			http.Error(w, "scanner state changed; retry", http.StatusConflict)
		case errors.Is(err, errWalletNoteStatusMempoolNotReady):
			http.Error(w, "scanner pending-spend state not ready", http.StatusServiceUnavailable)
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			http.Error(w, "scanner temporarily unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "db error", http.StatusInternalServerError)
		}
		return
	}
	if !snapshot.WalletFound {
		http.Error(w, "wallet not found", http.StatusNotFound)
		return
	}
	if !snapshot.TipFound {
		http.Error(w, "scanner not ready", http.StatusServiceUnavailable)
		return
	}
	if len(snapshot.EventEpoch) != store.EventEpochHexLength || !isLowerHex(snapshot.EventEpoch) || snapshot.AsOfScannerHeight < 0 ||
		len(snapshot.AsOfScannerHash) != 64 || !isLowerHex(snapshot.AsOfScannerHash) {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	statuses := make([]apiWalletNoteStatus, 0, len(parsed))
	for _, requested := range parsed {
		note, found := snapshot.Notes[requested.Ref]
		if !found {
			statuses = append(statuses, apiWalletNoteStatus{NoteID: requested.ID, State: "unknown"})
			continue
		}
		status, err := walletNoteStatusFromStore(requested, walletID, snapshot.AsOfScannerHeight, note)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		statuses = append(statuses, status)
	}

	writeJSON(w, apiWalletNoteStatusesResponse{
		WalletID:          walletID,
		EventEpoch:        snapshot.EventEpoch,
		AsOfScannerHeight: snapshot.AsOfScannerHeight,
		AsOfScannerHash:   snapshot.AsOfScannerHash,
		Statuses:          statuses,
	})
}

func writeWalletNoteStatusDecodeError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "invalid json", http.StatusBadRequest)
}

func parseSourceNoteID(noteID string) (store.NoteRef, bool) {
	if len(noteID) < 66 || len(noteID) > 75 || noteID[64] != ':' {
		return store.NoteRef{}, false
	}
	txid := noteID[:64]
	if !isLowerHex(txid) {
		return store.NoteRef{}, false
	}
	action := noteID[65:]
	if action == "" || (len(action) > 1 && action[0] == '0') {
		return store.NoteRef{}, false
	}
	for i := range action {
		if action[i] < '0' || action[i] > '9' {
			return store.NoteRef{}, false
		}
	}
	actionIndex, err := strconv.ParseUint(action, 10, 32)
	if err != nil || strconv.FormatUint(actionIndex, 10) != action {
		return store.NoteRef{}, false
	}
	return store.NoteRef{TxID: txid, ActionIndex: uint32(actionIndex)}, true
}

func (s *Server) readStableWalletNoteStatuses(ctx context.Context, walletID string, refs []store.NoteRef) (store.WalletNoteStatusSnapshot, error) {
	lastErr := errWalletNoteStatusSnapshotChanged
	for attempt := 0; attempt < 2; attempt++ {
		beforeRefresh := s.mempoolRefreshStatus()
		beforeEpoch, err := s.st.EventEpoch(ctx)
		if err != nil {
			return store.WalletNoteStatusSnapshot{}, err
		}
		beforeTip, beforeTipFound, err := s.st.Tip(ctx)
		if err != nil {
			return store.WalletNoteStatusSnapshot{}, err
		}
		snapshot, err := s.st.WalletNoteStatuses(ctx, walletID, refs)
		if err != nil {
			return store.WalletNoteStatusSnapshot{}, err
		}
		afterTip, afterTipFound, err := s.st.Tip(ctx)
		if err != nil {
			return store.WalletNoteStatusSnapshot{}, err
		}
		afterEpoch, err := s.st.EventEpoch(ctx)
		if err != nil {
			return store.WalletNoteStatusSnapshot{}, err
		}
		afterRefresh := s.mempoolRefreshStatus()
		snapshotTip, snapshotTipFound := snapshotBlockTip(snapshot)
		if beforeEpoch == snapshot.EventEpoch && snapshot.EventEpoch == afterEpoch &&
			sameBlockTip(beforeTip, beforeTipFound, snapshotTip, snapshotTipFound) &&
			sameBlockTip(afterTip, afterTipFound, snapshotTip, snapshotTipFound) {
			if snapshotTipFound && refreshMatches(beforeRefresh, snapshot.EventEpoch, snapshotTip) && refreshMatches(afterRefresh, snapshot.EventEpoch, snapshotTip) {
				return snapshot, nil
			}
			lastErr = errWalletNoteStatusMempoolNotReady
			continue
		}
		lastErr = errWalletNoteStatusSnapshotChanged
	}
	return store.WalletNoteStatusSnapshot{}, lastErr
}

func (s *Server) mempoolRefreshStatus() MempoolRefreshStatus {
	if s.mempoolRefresh == nil {
		return MempoolRefreshStatus{}
	}
	return s.mempoolRefresh()
}

func refreshMatches(refresh MempoolRefreshStatus, eventEpoch string, tip store.BlockTip) bool {
	return refresh.Ready && refresh.EventEpoch == eventEpoch && refresh.Height == tip.Height && refresh.Hash == tip.Hash
}

func snapshotBlockTip(snapshot store.WalletNoteStatusSnapshot) (store.BlockTip, bool) {
	return store.BlockTip{Height: snapshot.AsOfScannerHeight, Hash: snapshot.AsOfScannerHash}, snapshot.TipFound
}

func sameBlockTip(left store.BlockTip, leftFound bool, right store.BlockTip, rightFound bool) bool {
	return leftFound == rightFound && (!leftFound || left == right)
}

func walletNoteStatusFromStore(requested parsedWalletNoteStatusID, walletID string, asOfScannerHeight int64, note store.Note) (apiWalletNoteStatus, error) {
	if note.WalletID != walletID || note.TxID != requested.Ref.TxID || note.ActionIndex < 0 || uint32(note.ActionIndex) != requested.Ref.ActionIndex ||
		note.Height < 0 || note.Height > asOfScannerHeight || note.ValueZat < 0 ||
		(note.ConfirmedHeight != nil && (*note.ConfirmedHeight < note.Height || *note.ConfirmedHeight > asOfScannerHeight)) {
		return apiWalletNoteStatus{}, store.ErrInvalidWalletNoteState
	}
	height := note.Height
	value := note.ValueZat
	status := apiWalletNoteStatus{NoteID: requested.ID, SourceHeight: &height, ValueZat: &value}

	anyPending := note.PendingSpentTxID != nil || note.PendingSpentAt != nil || note.PendingSpentExpiryHeight != nil
	anySpent := note.SpentTxID != nil || note.SpentHeight != nil || note.SpentConfirmedHeight != nil
	if anyPending && anySpent {
		return apiWalletNoteStatus{}, store.ErrInvalidWalletNoteState
	}
	if anySpent {
		if note.SpentTxID == nil || note.SpentHeight == nil || len(*note.SpentTxID) != 64 || !isLowerHex(*note.SpentTxID) || *note.SpentHeight < note.Height || *note.SpentHeight > asOfScannerHeight ||
			(note.SpentConfirmedHeight != nil && (*note.SpentConfirmedHeight < *note.SpentHeight || *note.SpentConfirmedHeight > asOfScannerHeight)) {
			return apiWalletNoteStatus{}, store.ErrInvalidWalletNoteState
		}
		status.State = "spent"
		status.SpentTxID = note.SpentTxID
		status.SpentHeight = note.SpentHeight
		status.SpentConfirmedHeight = note.SpentConfirmedHeight
		return status, nil
	}
	if anyPending {
		if note.PendingSpentTxID == nil || note.PendingSpentAt == nil || len(*note.PendingSpentTxID) != 64 || !isLowerHex(*note.PendingSpentTxID) || note.PendingSpentAt.IsZero() ||
			(note.PendingSpentExpiryHeight != nil && *note.PendingSpentExpiryHeight < asOfScannerHeight) {
			return apiWalletNoteStatus{}, store.ErrInvalidWalletNoteState
		}
		status.State = "pending"
		status.PendingSpentTxID = note.PendingSpentTxID
		status.PendingSpentAt = note.PendingSpentAt
		status.PendingSpentExpiryHeight = note.PendingSpentExpiryHeight
		return status, nil
	}
	status.State = "unspent"
	return status, nil
}

func (s *Server) handleAddressBalance(w http.ResponseWriter, r *http.Request, walletID, recipientAddress string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}
	recipientAddress = strings.TrimSpace(recipientAddress)
	if recipientAddress == "" || len(recipientAddress) > 1024 || strings.Contains(recipientAddress, "/") {
		http.Error(w, "invalid recipient_address", http.StatusBadRequest)
		return
	}
	minConfirmations := s.defaultConfirmations
	if v := strings.TrimSpace(r.URL.Query().Get("min_confirmations")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 || n > 1_000_000 {
			http.Error(w, "invalid min_confirmations", http.StatusBadRequest)
			return
		}
		minConfirmations = n
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tip, ok, err := s.st.Tip(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "scanner not ready", http.StatusServiceUnavailable)
		return
	}
	balance, err := s.st.AddressBalance(ctx, walletID, recipientAddress, minConfirmations, tip.Height)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if !balance.WalletFound {
		http.Error(w, "wallet not found", http.StatusNotFound)
		return
	}
	nodeHeight := tip.Height
	if s.nodeHeight != nil {
		if h, known := s.nodeHeight(); known {
			nodeHeight = h
		}
	}
	lag := nodeHeight - tip.Height
	if lag < 0 {
		lag = 0
	}
	writeJSON(w, map[string]any{
		"wallet_id":            walletID,
		"recipient_address":    recipientAddress,
		"available_zat":        balance.AvailableZat,
		"pending_incoming_zat": balance.PendingIncomingZat,
		"pending_outgoing_zat": balance.PendingOutgoingZat,
		"total_unspent_zat":    balance.TotalUnspentZat,
		"min_confirmations":    minConfirmations,
		"as_of_node_height":    nodeHeight,
		"as_of_scanner_height": tip.Height,
		"scanner_lag":          lag,
	})
}

type walletRequest struct {
	WalletID       string `json:"wallet_id"`
	UFVK           string `json:"ufvk"`
	BirthdayHeight *int64 `json:"birthday_height,omitempty"`
}

type walletBackfillStore interface {
	UpsertWalletBirthday(context.Context, string, string, int64) error
	WalletBackfillStatus(context.Context, string) (store.WalletBackfillProgress, bool, error)
	SetWalletBackfillProgress(context.Context, store.WalletBackfillProgress) error
}

func (s *Server) handleUpsertWallet(w http.ResponseWriter, r *http.Request) {
	var req walletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	req.WalletID = strings.TrimSpace(req.WalletID)
	req.UFVK = strings.TrimSpace(req.UFVK)
	if req.WalletID == "" || req.UFVK == "" {
		http.Error(w, "wallet_id and ufvk are required", http.StatusBadRequest)
		return
	}
	birthdayHeight := int64(0)
	if req.BirthdayHeight != nil {
		birthdayHeight = *req.BirthdayHeight
	}
	if birthdayHeight < 0 {
		http.Error(w, "invalid birthday_height", http.StatusBadRequest)
		return
	}
	if !isSafeWalletID(req.WalletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := orchardscan.ValidateUFVK(ctx, req.UFVK); err != nil {
		var oe *orchardscan.Error
		if errors.As(err, &oe) {
			switch oe.Code {
			case orchardscan.ErrUFVKInvalid,
				orchardscan.ErrUFVKMissingOrchardReceiver,
				orchardscan.ErrUFVKOrchardFVKLenInvalid,
				orchardscan.ErrUFVKOrchardFVKBytesInvalid:
				http.Error(w, string(oe.Code), http.StatusBadRequest)
				return
			default:
				http.Error(w, "ufvk validation error", http.StatusInternalServerError)
				return
			}
		}
		http.Error(w, "ufvk validation error", http.StatusInternalServerError)
		return
	}

	var err error
	if req.BirthdayHeight == nil {
		err = s.st.UpsertWallet(ctx, req.WalletID, req.UFVK)
	} else if bfs, ok := s.st.(walletBackfillStore); ok {
		err = bfs.UpsertWalletBirthday(ctx, req.WalletID, req.UFVK, birthdayHeight)
	} else {
		err = errors.New("birthday height unsupported")
	}
	if err != nil {
		if errors.Is(err, store.ErrWalletUFVKMismatch) || errors.Is(err, store.ErrUFVKAlreadyRegistered) || errors.Is(err, store.ErrBirthdayIncrease) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	wallets, err := s.st.ListWallets(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	for _, wallet := range wallets {
		if wallet.WalletID == req.WalletID {
			birthdayHeight = wallet.BirthdayHeight
			break
		}
	}
	writeJSON(w, map[string]any{"status": "ok", "birthday_height": birthdayHeight, "ufvk_fingerprint": store.UFVKFingerprint(req.UFVK)})
}

func (s *Server) handleListWallets(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	type wallet struct {
		WalletID        string     `json:"wallet_id"`
		UFVKFingerprint string     `json:"ufvk_fingerprint"`
		BirthdayHeight  int64      `json:"birthday_height"`
		CreatedAt       time.Time  `json:"created_at"`
		DisabledAt      *time.Time `json:"disabled_at,omitempty"`
	}

	wallets, err := s.st.ListWallets(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	out := make([]wallet, 0, len(wallets))
	for _, w0 := range wallets {
		out = append(out, wallet{
			WalletID:        w0.WalletID,
			UFVKFingerprint: w0.UFVKFingerprint,
			BirthdayHeight:  w0.BirthdayHeight,
			CreatedAt:       w0.CreatedAt,
			DisabledAt:      w0.DisabledAt,
		})
	}

	writeJSON(w, map[string]any{"wallets": out})
}

func (s *Server) handleListWalletEvents(w http.ResponseWriter, r *http.Request, walletID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}

	cursor := int64(0)
	if rawCursor := strings.TrimSpace(r.URL.Query().Get("cursor")); rawCursor != "" {
		parsed, err := strconv.ParseInt(rawCursor, 10, 64)
		if err != nil || parsed < 0 {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		cursor = parsed
	}
	limit := parseInt64Query(r, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var blockHeight *int64
	if v := strings.TrimSpace(r.URL.Query().Get("block_height")); v != "" {
		h, err := strconv.ParseInt(v, 10, 64)
		if err != nil || h < 0 {
			http.Error(w, "invalid block_height", http.StatusBadRequest)
			return
		}
		blockHeight = &h
	}

	var kinds []string
	{
		seen := make(map[string]struct{})
		for _, raw := range r.URL.Query()["kind"] {
			for _, k := range strings.Split(raw, ",") {
				k = strings.TrimSpace(k)
				if k == "" {
					continue
				}
				if canon, ok := canonicalEventKind(k); ok {
					k = canon
				}
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = struct{}{}
				kinds = append(kinds, k)
			}
		}
	}

	txid := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("txid")))
	if txid != "" {
		if len(txid) != 64 || !isLowerHex(txid) {
			http.Error(w, "invalid txid", http.StatusBadRequest)
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	type event struct {
		ID        int64           `json:"id"`
		Kind      string          `json:"kind"`
		Height    int64           `json:"height"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}

	filter := store.EventFilter{
		BlockHeight: blockHeight,
		Kinds:       kinds,
		TxID:        txid,
	}

	var evs []store.Event
	var nextCursor int64
	var eventEpoch string
	for attempt := 0; attempt < 3; attempt++ {
		before, err := s.st.EventEpoch(ctx)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		maxEventID, err := s.st.MaxWalletEventID(ctx, walletID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if cursor > maxEventID {
			http.Error(w, "event cursor exceeds durable journal; verify event_epoch and reset cursor", http.StatusConflict)
			return
		}
		evs, nextCursor, err = s.st.ListWalletEvents(ctx, walletID, cursor, int(limit), filter)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		after, err := s.st.EventEpoch(ctx)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if before == after && len(after) == store.EventEpochHexLength && isLowerHex(after) {
			eventEpoch = after
			break
		}
	}
	if eventEpoch == "" {
		http.Error(w, "event journal changed; retry", http.StatusConflict)
		return
	}

	events := make([]event, 0, len(evs))
	for _, e := range evs {
		events = append(events, event{
			ID:        e.ID,
			Kind:      e.Kind,
			Height:    e.Height,
			Payload:   e.Payload,
			CreatedAt: e.CreatedAt,
		})
	}

	writeJSON(w, map[string]any{
		"event_epoch": eventEpoch,
		"events":      events,
		"next_cursor": nextCursor,
	})
}

func canonicalEventKind(kind string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case strings.ToLower(events.KindDepositEvent):
		return events.KindDepositEvent, true
	case strings.ToLower(events.KindDepositConfirmed):
		return events.KindDepositConfirmed, true
	case strings.ToLower(events.KindDepositOrphaned):
		return events.KindDepositOrphaned, true
	case strings.ToLower(events.KindDepositUnconfirmed):
		return events.KindDepositUnconfirmed, true
	case strings.ToLower(events.KindSpendEvent):
		return events.KindSpendEvent, true
	case strings.ToLower(events.KindSpendConfirmed):
		return events.KindSpendConfirmed, true
	case strings.ToLower(events.KindSpendOrphaned):
		return events.KindSpendOrphaned, true
	case strings.ToLower(events.KindSpendUnconfirmed):
		return events.KindSpendUnconfirmed, true
	case strings.ToLower(events.KindOutgoingOutputEvent):
		return events.KindOutgoingOutputEvent, true
	case strings.ToLower(events.KindOutgoingOutputConfirmed):
		return events.KindOutgoingOutputConfirmed, true
	case strings.ToLower(events.KindOutgoingOutputOrphaned):
		return events.KindOutgoingOutputOrphaned, true
	case strings.ToLower(events.KindOutgoingOutputUnconfirmed):
		return events.KindOutgoingOutputUnconfirmed, true
	case strings.ToLower(events.KindOutgoingOutputExpired):
		return events.KindOutgoingOutputExpired, true
	default:
		return "", false
	}
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			continue
		}
		if c >= 'a' && c <= 'f' {
			continue
		}
		return false
	}
	return true
}

func (s *Server) handleBackfillWallet(w http.ResponseWriter, r *http.Request, walletID string) {
	if r.Method == http.MethodGet {
		s.handleBackfillStatus(w, r, walletID)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}
	if s.bf == nil {
		http.Error(w, "backfill not configured", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		FromHeight *int64 `json:"from_height,omitempty"`
		ToHeight   *int64 `json:"to_height,omitempty"`
		BatchSize  int64  `json:"batch_size,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if !errors.Is(err, io.EOF) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	fromHeight := int64(0)
	progressStore, hasProgressStore := s.st.(walletBackfillStore)
	progress, progressFound := store.WalletBackfillProgress{}, false
	var err error
	if hasProgressStore {
		progress, progressFound, err = progressStore.WalletBackfillStatus(ctx, walletID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}
	if !hasProgressStore || !progressFound {
		http.Error(w, "wallet backfill progress unavailable", http.StatusConflict)
		return
	}
	fromHeight = progress.NextHeight
	if req.FromHeight != nil && *req.FromHeight != progress.NextHeight {
		http.Error(w, "from_height must equal persisted next_height", http.StatusConflict)
		return
	}
	wallets, err := s.st.ListWallets(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	identityValid := false
	for _, wallet := range wallets {
		if wallet.WalletID == walletID && wallet.DisabledAt == nil && wallet.BirthdayHeight == progress.BirthdayHeight && wallet.UFVKFingerprint == progress.UFVKFingerprint {
			identityValid = true
			break
		}
	}
	if !identityValid || progress.Generation < 1 || progress.NextHeight < progress.BirthdayHeight {
		http.Error(w, "wallet backfill identity changed", http.StatusConflict)
		return
	}

	tip, ok, err := s.st.Tip(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "scanner has not indexed any blocks yet", http.StatusBadRequest)
		return
	}

	toHeight := tip.Height
	if req.ToHeight != nil {
		toHeight = *req.ToHeight
	}
	if toHeight > tip.Height {
		http.Error(w, "to_height exceeds scanned tip", http.StatusBadRequest)
		return
	}
	if toHeight < fromHeight {
		if req.ToHeight == nil && progress.State == "complete" && fromHeight > tip.Height {
			writeJSON(w, map[string]any{
				"status":                 "ok",
				"wallet_id":              walletID,
				"from_height":            fromHeight,
				"to_height":              tip.Height,
				"scanned_from":           fromHeight,
				"scanned_to":             tip.Height,
				"next_height":            fromHeight,
				"inserted_notes":         0,
				"inserted_events":        0,
				"visited_action_heights": 0,
				"skipped_heights":        0,
				"rpc_calls":              0,
			})
			return
		}
		http.Error(w, "to_height must be >= persisted next_height", http.StatusBadRequest)
		return
	}

	res, err := s.bf.BackfillWallet(ctx, backfill.Request{
		WalletID:   walletID,
		FromHeight: fromHeight,
		ToHeight:   toHeight,
		BatchSize:  req.BatchSize,
	})
	if err != nil {
		if hasProgressStore && progressFound {
			expected := fromHeight
			progress.ExpectedNextHeight = &expected
			progress.State, progress.LastError, progress.TargetHeight = "error", err.Error(), tip.Height
			_ = progressStore.SetWalletBackfillProgress(ctx, progress)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	next := res.NextHeight
	if res.ScannedFrom != fromHeight || next <= fromHeight || next > tip.Height+1 {
		http.Error(w, "invalid backfill progress result", http.StatusConflict)
		return
	}
	if hasProgressStore {
		expected := fromHeight
		progress.ExpectedNextHeight = &expected
		progress.NextHeight, progress.TargetHeight, progress.LastError = next, tip.Height, ""
		progress.State = "running"
		if next > tip.Height {
			progress.State = "complete"
		}
		if err := progressStore.SetWalletBackfillProgress(ctx, progress); err != nil {
			if errors.Is(err, store.ErrBackfillProgressConflict) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if progress.State == "complete" {
			latestTip, found, err := s.st.Tip(ctx)
			if err != nil {
				http.Error(w, "db error", http.StatusInternalServerError)
				return
			}
			if found && latestTip.Height >= next {
				if err := s.st.WithTx(ctx, func(tx store.Tx) error {
					return tx.AdvanceCompleteWalletBackfillProgress(ctx, latestTip.Height)
				}); err != nil {
					http.Error(w, "db error", http.StatusInternalServerError)
					return
				}
				if refreshed, found, err := progressStore.WalletBackfillStatus(ctx, walletID); err != nil {
					http.Error(w, "db error", http.StatusInternalServerError)
					return
				} else if found && refreshed.Generation == progress.Generation {
					next = refreshed.NextHeight
				}
			}
		}
	}
	writeJSON(w, map[string]any{
		"status":                 "ok",
		"wallet_id":              walletID,
		"from_height":            res.FromHeight,
		"to_height":              res.ToHeight,
		"scanned_from":           res.ScannedFrom,
		"scanned_to":             res.ScannedTo,
		"next_height":            next,
		"inserted_notes":         res.InsertedNotes,
		"inserted_events":        res.InsertedEvents,
		"visited_action_heights": res.VisitedActionHeights,
		"skipped_heights":        res.SkippedHeights,
		"rpc_calls":              res.RPCCalls,
	})
}

func (s *Server) handleBackfillStatus(w http.ResponseWriter, r *http.Request, walletID string) {
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}
	progressStore, ok := s.st.(walletBackfillStore)
	if !ok {
		http.Error(w, "backfill status unsupported", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	progress, found, err := progressStore.WalletBackfillStatus(ctx, walletID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "wallet not found", http.StatusNotFound)
		return
	}
	writeJSON(w, progress)
}

func (s *Server) handleListWalletNotes(w http.ResponseWriter, r *http.Request, walletID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if walletID == "" || !isSafeWalletID(walletID) {
		http.Error(w, "invalid wallet_id", http.StatusBadRequest)
		return
	}

	spent := r.URL.Query().Get("spent")
	onlyUnspent := spent == "" || spent == "false"

	limit := int64(1000)
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 || n > 1000 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = n
	}

	minValueZat := int64(0)
	if v := strings.TrimSpace(r.URL.Query().Get("min_value_zat")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid min_value_zat", http.StatusBadRequest)
			return
		}
		minValueZat = n
	}
	recipientAddress := strings.TrimSpace(r.URL.Query().Get("recipient_address"))
	if len(recipientAddress) > 1024 || strings.Contains(recipientAddress, "/") {
		http.Error(w, "invalid recipient_address", http.StatusBadRequest)
		return
	}

	direction := noteDirectionAll
	if v := strings.TrimSpace(r.URL.Query().Get("direction")); v != "" {
		switch noteDirection(strings.ToLower(v)) {
		case noteDirectionIncoming, noteDirectionOutgoing, noteDirectionAll:
			direction = noteDirection(strings.ToLower(v))
		default:
			http.Error(w, "invalid direction", http.StatusBadRequest)
			return
		}
	}

	cursor, err := parseNotesCursor(strings.TrimSpace(r.URL.Query().Get("cursor")))
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var (
		incomingRows []store.Note
		outgoingRows []store.OutgoingOutput
		incomingMore *store.NotesCursor
		outgoingMore *store.NotesCursor
	)

	if direction != noteDirectionOutgoing {
		incomingRows, incomingMore, err = s.st.ListWalletNotesPage(ctx, walletID, store.NotesQuery{
			OnlyUnspent:      onlyUnspent,
			MinValueZat:      minValueZat,
			RecipientAddress: recipientAddress,
			Limit:            int(limit),
			Cursor:           notesCursorTriple(cursor),
		})
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}

	if direction != noteDirectionIncoming {
		outgoingRows, outgoingMore, err = s.st.ListWalletOutgoingOutputsPage(ctx, walletID, store.OutgoingOutputsQuery{
			MinValueZat:   minValueZat,
			Limit:         int(limit),
			Cursor:        notesCursorTriple(cursor),
			IncludeCursor: cursor != nil && cursor.Direction == noteDirectionIncoming,
		})
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}

	notes := make([]apiWalletNote, 0, limit)
	inIdx := 0
	outIdx := 0
	for len(notes) < int(limit) {
		var next apiWalletNote
		hasNext := false

		if inIdx < len(incomingRows) {
			n := incomingRows[inIdx]
			noteNullifier := n.NoteNullifier
			next = apiWalletNote{
				Direction:           noteDirectionIncoming,
				TxID:                n.TxID,
				ActionIndex:         n.ActionIndex,
				Height:              n.Height,
				Position:            n.Position,
				Recipient:           n.RecipientAddress,
				ValueZat:            n.ValueZat,
				MemoHex:             n.MemoHex,
				NoteNullifier:       &noteNullifier,
				PendingTxID:         n.PendingSpentTxID,
				PendingAt:           n.PendingSpentAt,
				PendingExpiryHeight: n.PendingSpentExpiryHeight,
				SpentHeight:         n.SpentHeight,
				SpentTxID:           n.SpentTxID,
				CreatedAt:           n.CreatedAt,
			}
			hasNext = true
		}

		if outIdx < len(outgoingRows) {
			o := outgoingRows[outIdx]
			if o.MinedHeight == nil {
				http.Error(w, "db error", http.StatusInternalServerError)
				return
			}
			ovkScope := o.OvkScope
			candidate := apiWalletNote{
				Direction:      noteDirectionOutgoing,
				TxID:           o.TxID,
				ActionIndex:    o.ActionIndex,
				Height:         *o.MinedHeight,
				Position:       o.Position,
				Recipient:      o.RecipientAddress,
				ValueZat:       o.ValueZat,
				MemoHex:        o.MemoHex,
				OvkScope:       &ovkScope,
				RecipientScope: o.RecipientScope,
				CreatedAt:      o.CreatedAt,
			}
			if !hasNext || compareAPINotes(candidate, next) < 0 {
				next = candidate
				hasNext = true
				outIdx++
			} else {
				inIdx++
			}
		} else if hasNext {
			inIdx++
		}

		if !hasNext {
			break
		}

		notes = append(notes, next)
	}

	hasMore := inIdx < len(incomingRows) || outIdx < len(outgoingRows) || incomingMore != nil || outgoingMore != nil

	resp := map[string]any{"notes": notes}
	if hasMore && len(notes) > 0 {
		last := notes[len(notes)-1]
		resp["next_cursor"] = encodeNotesCursor(notesCursor{
			Height:      last.Height,
			TxID:        last.TxID,
			ActionIndex: last.ActionIndex,
			Direction:   last.Direction,
		})
	}
	writeJSON(w, resp)
}

type witnessRequest struct {
	AnchorHeight *int64   `json:"anchor_height,omitempty"`
	Positions    []uint32 `json:"positions"`
}

func (s *Server) handleOrchardWitness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req witnessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(req.Positions) == 0 || len(req.Positions) > 1000 {
		http.Error(w, "positions must be between 1 and 1000", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	anchorHeight := int64(0)
	if req.AnchorHeight != nil {
		anchorHeight = *req.AnchorHeight
	} else {
		tip, ok, err := s.st.Tip(ctx)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "no scanned blocks", http.StatusBadRequest)
			return
		}
		anchorHeight = tip.Height
	}
	if anchorHeight < 0 {
		http.Error(w, "anchor_height must be >= 0", http.StatusBadRequest)
		return
	}

	res, err := s.computeOrchardWitness(ctx, anchorHeight, req.Positions)
	if err != nil {
		if errors.Is(err, errWitnessAnchorChanged) {
			http.Error(w, "witness anchor changed; retry", http.StatusConflict)
			return
		}
		if errors.Is(err, errDBAccess) {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if errors.Is(err, errNoCommitments) {
			http.Error(w, "no commitments", http.StatusBadRequest)
			return
		}
		if errors.Is(err, errInvalidCommitmentPositions) {
			http.Error(w, "invalid commitment positions", http.StatusInternalServerError)
			return
		}
		var oe *orchardscan.Error
		if errors.As(err, &oe) && oe.Code == orchardscan.ErrInvalidRequest {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		http.Error(w, "witness error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"status":              "ok",
		"anchor_height":       anchorHeight,
		"root":                res.Root,
		"paths":               res.Paths,
		"compute_mode":        res.ComputeMode,
		"fallback_from":       res.FallbackFrom,
		"fallback_reason":     res.FallbackReason,
		"streamed_leaf_count": res.StreamedLeafCount,
		"inserted_root_count": res.InsertedRootCount,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	_ = enc.Encode(v)
}

func parseInt64Query(r *http.Request, key string, def int64) int64 {
	v := strings.TrimSpace(r.URL.Query().Get(key))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

type notesCursor struct {
	Height      int64
	TxID        string
	ActionIndex int32
	Direction   noteDirection
}

func encodeNotesCursor(cursor notesCursor) string {
	txid := strings.ToLower(strings.TrimSpace(cursor.TxID))
	return strconv.FormatInt(cursor.Height, 10) + ":" + txid + ":" + strconv.FormatInt(int64(cursor.ActionIndex), 10) + ":" + string(cursor.Direction)
}

func parseNotesCursor(raw string) (*notesCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 && len(parts) != 4 {
		return nil, errors.New("invalid cursor")
	}
	height, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || height < 0 {
		return nil, errors.New("invalid cursor")
	}
	txid := strings.ToLower(strings.TrimSpace(parts[1]))
	if len(txid) != 64 || !isLowerHex(txid) {
		return nil, errors.New("invalid cursor")
	}
	actionIndex, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil || actionIndex < 0 {
		return nil, errors.New("invalid cursor")
	}
	direction := noteDirectionIncoming
	if len(parts) == 4 {
		switch noteDirection(strings.ToLower(strings.TrimSpace(parts[3]))) {
		case noteDirectionIncoming, noteDirectionOutgoing:
			direction = noteDirection(strings.ToLower(strings.TrimSpace(parts[3])))
		default:
			return nil, errors.New("invalid cursor")
		}
	}
	return &notesCursor{
		Height:      height,
		TxID:        txid,
		ActionIndex: int32(actionIndex),
		Direction:   direction,
	}, nil
}

func notesCursorTriple(cursor *notesCursor) *store.NotesCursor {
	if cursor == nil {
		return nil
	}
	return &store.NotesCursor{
		Height:      cursor.Height,
		TxID:        cursor.TxID,
		ActionIndex: cursor.ActionIndex,
	}
}

func compareAPINotes(a, b apiWalletNote) int {
	if a.Height != b.Height {
		if a.Height < b.Height {
			return -1
		}
		return 1
	}
	if a.TxID != b.TxID {
		if a.TxID < b.TxID {
			return -1
		}
		return 1
	}
	if a.ActionIndex != b.ActionIndex {
		if a.ActionIndex < b.ActionIndex {
			return -1
		}
		return 1
	}
	return compareNoteDirections(a.Direction, b.Direction)
}

func compareNoteDirections(a, b noteDirection) int {
	rank := func(v noteDirection) int {
		switch v {
		case noteDirectionIncoming:
			return 0
		case noteDirectionOutgoing:
			return 1
		default:
			return 2
		}
	}
	ra := rank(a)
	rb := rank(b)
	switch {
	case ra < rb:
		return -1
	case ra > rb:
		return 1
	default:
		return 0
	}
}

func isSafeWalletID(s string) bool {
	if len(s) > 64 {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= 'A' && c <= 'Z' {
			continue
		}
		if c >= '0' && c <= '9' {
			continue
		}
		if c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func validBearerToken(r *http.Request, expected string) bool {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" {
		return false
	}
	parts := strings.Fields(auth)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return false
	}
	got := parts[1]
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
