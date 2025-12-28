package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/orchardscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) (*Server, error) {
	if db == nil {
		return nil, errors.New("api: db is nil")
	}
	return &Server{db: db}, nil
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

	var tipHeight int64
	var tipHash string
	if err := s.db.QueryRow(ctx, `SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1`).Scan(&tipHeight, &tipHash); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	} else {
		resp["scanned_height"] = tipHeight
		resp["scanned_hash"] = tipHash
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

	_, err := s.db.Exec(ctx, `
INSERT INTO wallets (wallet_id, ufvk, disabled_at)
VALUES ($1, $2, NULL)
ON CONFLICT (wallet_id)
DO UPDATE SET ufvk = EXCLUDED.ufvk, disabled_at = NULL
`, req.WalletID, req.UFVK)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"status": "ok"})
}

func (s *Server) handleListWallets(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := s.db.Query(ctx, `SELECT wallet_id, created_at, disabled_at FROM wallets ORDER BY wallet_id`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type wallet struct {
		WalletID   string     `json:"wallet_id"`
		CreatedAt  time.Time  `json:"created_at"`
		DisabledAt *time.Time `json:"disabled_at,omitempty"`
	}
	var out []wallet
	for rows.Next() {
		var w0 wallet
		if err := rows.Scan(&w0.WalletID, &w0.CreatedAt, &w0.DisabledAt); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		out = append(out, w0)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
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

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	rows, err := s.db.Query(ctx, `
SELECT id, kind, height, payload, created_at
FROM events
WHERE wallet_id = $1 AND id > $2
ORDER BY id
LIMIT $3
`, walletID, cursor, limit)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type event struct {
		ID        int64           `json:"id"`
		Kind      string          `json:"kind"`
		Height    int64           `json:"height"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}
	var events []event
	var nextCursor int64 = cursor
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.ID, &e.Kind, &e.Height, &e.Payload, &e.CreatedAt); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		nextCursor = e.ID
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"events":      events,
		"next_cursor": nextCursor,
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

	query := `
SELECT txid, action_index, height, position, recipient_address, value_zat, note_nullifier, spent_height, spent_txid, created_at
FROM notes
WHERE wallet_id = $1
`
	if onlyUnspent {
		query += " AND spent_height IS NULL"
	}
	query += " ORDER BY height, txid, action_index LIMIT 1000"

	rows, err := s.db.Query(ctx, query, walletID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

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

	var notes []note
	for rows.Next() {
		var n note
		if err := rows.Scan(
			&n.TxID,
			&n.ActionIndex,
			&n.Height,
			&n.Position,
			&n.Recipient,
			&n.ValueZat,
			&n.NoteNullifier,
			&n.SpentHeight,
			&n.SpentTxID,
			&n.CreatedAt,
		); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		notes = append(notes, n)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
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
		if err := s.db.QueryRow(ctx, `SELECT height FROM blocks ORDER BY height DESC LIMIT 1`).Scan(&anchorHeight); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "no scanned blocks", http.StatusBadRequest)
				return
			}
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}
	if anchorHeight < 0 {
		http.Error(w, "anchor_height must be >= 0", http.StatusBadRequest)
		return
	}

	rows, err := s.db.Query(ctx, `
SELECT position, cmx
FROM orchard_commitments
WHERE height <= $1
ORDER BY position
`, anchorHeight)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var expectedPos int64 = 0
	cmxHex := make([]string, 0, 1024)
	for rows.Next() {
		var pos int64
		var cmx string
		if err := rows.Scan(&pos, &cmx); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		if pos != expectedPos {
			http.Error(w, "invalid commitment positions", http.StatusInternalServerError)
			return
		}
		expectedPos++
		cmxHex = append(cmxHex, cmx)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	if len(cmxHex) == 0 {
		http.Error(w, "no commitments", http.StatusBadRequest)
		return
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
