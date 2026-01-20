package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/backfill"
	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/Abdullah1738/juno-scan/internal/store"
)

type Server struct {
	st store.Store
	bf *backfill.Service
}

type Option func(*Server)

func WithBackfillService(bf *backfill.Service) Option {
	return func(s *Server) {
		s.bf = bf
	}
}

func New(st store.Store, opts ...Option) (*Server, error) {
	if st == nil {
		return nil, errors.New("api: store is nil")
	}
	s := &Server{st: st}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/orchard/witness", s.handleOrchardWitness)
	mux.HandleFunc("/v1/wallets", s.handleWallets)
	mux.HandleFunc("/v1/wallets/", s.handleWalletSubroutes)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp := map[string]any{
		"status": "ok",
	}

	if tip, ok, err := s.st.Tip(ctx); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	} else if ok {
		resp["scanned_height"] = tip.Height
		resp["scanned_hash"] = tip.Hash
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
		s.handleListWalletNotes(w, r, walletID)
	case "backfill":
		s.handleBackfillWallet(w, r, walletID)
	default:
		http.NotFound(w, r)
	}
}

type walletRequest struct {
	WalletID string `json:"wallet_id"`
	UFVK     string `json:"ufvk"`
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

	if err := s.st.UpsertWallet(ctx, req.WalletID, req.UFVK); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"status": "ok"})
}

func (s *Server) handleListWallets(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	type wallet struct {
		WalletID   string     `json:"wallet_id"`
		CreatedAt  time.Time  `json:"created_at"`
		DisabledAt *time.Time `json:"disabled_at,omitempty"`
	}

	wallets, err := s.st.ListWallets(ctx)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	out := make([]wallet, 0, len(wallets))
	for _, w0 := range wallets {
		out = append(out, wallet{
			WalletID:   w0.WalletID,
			CreatedAt:  w0.CreatedAt,
			DisabledAt: w0.DisabledAt,
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

	cursor := parseInt64Query(r, "cursor", 0)
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

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	type event struct {
		ID        int64           `json:"id"`
		Kind      string          `json:"kind"`
		Height    int64           `json:"height"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}

	evs, nextCursor, err := s.st.ListWalletEvents(ctx, walletID, cursor, int(limit), blockHeight)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
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
		"events":      events,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleBackfillWallet(w http.ResponseWriter, r *http.Request, walletID string) {
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
		FromHeight int64  `json:"from_height"`
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

	res, err := s.bf.BackfillWallet(ctx, backfill.Request{
		WalletID:   walletID,
		FromHeight: req.FromHeight,
		ToHeight:   toHeight,
		BatchSize:  req.BatchSize,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	next := res.NextHeight
	writeJSON(w, map[string]any{
		"status":          "ok",
		"wallet_id":       walletID,
		"from_height":     res.FromHeight,
		"to_height":       res.ToHeight,
		"scanned_from":    res.ScannedFrom,
		"scanned_to":      res.ScannedTo,
		"next_height":     next,
		"inserted_notes":  res.InsertedNotes,
		"inserted_events": res.InsertedEvents,
	})
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

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	type note struct {
		TxID          string    `json:"txid"`
		ActionIndex   int32     `json:"action_index"`
		Height        int64     `json:"height"`
		Position      *int64    `json:"position,omitempty"`
		Recipient     string    `json:"recipient_address"`
		ValueZat      int64     `json:"value_zat"`
		NoteNullifier string    `json:"note_nullifier"`
		SpentHeight   *int64    `json:"spent_height,omitempty"`
		SpentTxID     *string   `json:"spent_txid,omitempty"`
		CreatedAt     time.Time `json:"created_at"`
	}

	ns, err := s.st.ListWalletNotes(ctx, walletID, onlyUnspent, 1000)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	notes := make([]note, 0, len(ns))
	for _, n := range ns {
		notes = append(notes, note{
			TxID:          n.TxID,
			ActionIndex:   n.ActionIndex,
			Height:        n.Height,
			Position:      n.Position,
			Recipient:     n.RecipientAddress,
			ValueZat:      n.ValueZat,
			NoteNullifier: n.NoteNullifier,
			SpentHeight:   n.SpentHeight,
			SpentTxID:     n.SpentTxID,
			CreatedAt:     n.CreatedAt,
		})
	}

	writeJSON(w, map[string]any{"notes": notes})
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

	commitments, err := s.st.ListOrchardCommitmentsUpToHeight(ctx, anchorHeight)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if len(commitments) == 0 {
		http.Error(w, "no commitments", http.StatusBadRequest)
		return
	}

	var expectedPos int64 = 0
	cmxHex := make([]string, 0, len(commitments))
	for _, c := range commitments {
		if c.Position != expectedPos {
			http.Error(w, "invalid commitment positions", http.StatusInternalServerError)
			return
		}
		expectedPos++
		cmxHex = append(cmxHex, c.CMX)
	}

	res, err := orchardscan.OrchardWitness(ctx, cmxHex, req.Positions)
	if err != nil {
		var oe *orchardscan.Error
		if errors.As(err, &oe) && oe.Code == orchardscan.ErrInvalidRequest {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		http.Error(w, "witness error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"status":        "ok",
		"anchor_height": anchorHeight,
		"root":          res.Root,
		"paths":         res.Paths,
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
