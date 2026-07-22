package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrWalletUFVKMismatch       = errors.New("wallet_ufvk_immutable")
	ErrUFVKAlreadyRegistered    = errors.New("ufvk_already_registered")
	ErrBirthdayIncrease         = errors.New("birthday_height_increase_forbidden")
	ErrBackfillProgressConflict = errors.New("backfill_progress_conflict")
	ErrCanonicalBlockChanged    = errors.New("canonical_block_changed")
	ErrWalletNoteSummaryLimit   = errors.New("wallet_note_summary_limit_exceeded")
	ErrInvalidWalletNoteState   = errors.New("invalid_wallet_note_state")
	ErrInvalidNoteStatusQuery   = errors.New("invalid_note_status_query")
)

const MaxNoteStatusBatch = 200

type Store interface {
	Close() error
	Migrate(ctx context.Context) error
	EventEpoch(ctx context.Context) (string, error)
	RotateEventEpoch(ctx context.Context) (string, error)

	WithTx(ctx context.Context, fn func(Tx) error) error

	UpsertWallet(ctx context.Context, walletID, ufvk string) error
	ListWallets(ctx context.Context) ([]Wallet, error)
	ListEnabledWalletUFVKs(ctx context.Context) ([]WalletUFVK, error)

	Tip(ctx context.Context) (BlockTip, bool, error)
	HashAtHeight(ctx context.Context, height int64) (string, bool, error)
	RollbackToHeight(ctx context.Context, height int64) error

	WalletEventPublishCursor(ctx context.Context, walletID string) (int64, error)
	SetWalletEventPublishCursor(ctx context.Context, walletID string, cursor int64) error
	MaxWalletEventID(ctx context.Context, walletID string) (int64, error)

	ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int, filter EventFilter) (events []Event, nextCursor int64, err error)
	ListWalletNotesPage(ctx context.Context, walletID string, query NotesQuery) (notes []Note, nextCursor *NotesCursor, err error)
	WalletNoteStatuses(ctx context.Context, walletID string, refs []NoteRef) (WalletNoteStatusSnapshot, error)
	WalletNoteSummary(ctx context.Context, walletID string, minConfirmations, minNoteZat int64, maxNotes int) (WalletNoteSummary, error)
	AddressBalance(ctx context.Context, walletID, recipientAddress string, minConfirmations, scannerHeight int64) (AddressBalance, error)
	ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]Note, error)
	ListWalletOutgoingOutputsPage(ctx context.Context, walletID string, query OutgoingOutputsQuery) (outputs []OutgoingOutput, nextCursor *NotesCursor, err error)
	UpdatePendingSpends(ctx context.Context, pending map[string]PendingSpend, chainHeight int64, seenAt time.Time) error
	ListNotesByPendingSpentTxIDs(ctx context.Context, txids []string) ([]Note, error)
	ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]OrchardCommitment, error)
	FirstOrchardCommitmentPositionFromHeight(ctx context.Context, height int64) (pos int64, ok bool, err error)

	ListOutgoingOutputsByTxID(ctx context.Context, walletID, txid string) ([]OutgoingOutput, error)
}

type Tx interface {
	AssertCanonicalBlock(ctx context.Context, height int64, hash string) error
	AdvanceCompleteWalletBackfillProgress(ctx context.Context, scannedHeight int64) error
	InsertBlock(ctx context.Context, b Block) error
	NextOrchardCommitmentPosition(ctx context.Context) (int64, error)
	InsertOrchardAction(ctx context.Context, a OrchardAction) error
	InsertOrchardCommitment(ctx context.Context, c OrchardCommitment) error
	MarkNotesSpent(ctx context.Context, height int64, txid string, nullifiers []string) ([]Note, error)
	ConfirmNotes(ctx context.Context, confirmationHeight int64, maxNoteHeight int64) ([]Note, error)
	ConfirmSpends(ctx context.Context, confirmationHeight int64, maxSpentHeight int64) ([]Note, error)
	InsertNote(ctx context.Context, n Note) (inserted bool, err error)
	InsertOutgoingOutput(ctx context.Context, o OutgoingOutput) (changed bool, err error)
	ConfirmOutgoingOutputs(ctx context.Context, confirmationHeight int64, maxMinedHeight int64) ([]OutgoingOutput, error)
	ExpireOutgoingOutputs(ctx context.Context, chainHeight int64, expiredAt time.Time) ([]OutgoingOutput, error)
	InsertEvent(ctx context.Context, e Event) error
	MarkTransactionInternal(ctx context.Context, txid string) (bool, error)
}

type PendingSpend struct {
	TxID         string
	ExpiryHeight *int64
}

type Wallet struct {
	WalletID        string
	UFVKFingerprint string
	BirthdayHeight  int64
	CreatedAt       time.Time
	DisabledAt      *time.Time
}

type WalletBackfillProgress struct {
	WalletID           string    `json:"wallet_id"`
	UFVKFingerprint    string    `json:"ufvk_fingerprint"`
	BirthdayHeight     int64     `json:"birthday_height"`
	NextHeight         int64     `json:"next_height"`
	TargetHeight       int64     `json:"target_height"`
	State              string    `json:"state"`
	LastError          string    `json:"last_error,omitempty"`
	Generation         int64     `json:"generation"`
	UpdatedAt          time.Time `json:"updated_at"`
	ExpectedNextHeight *int64    `json:"-"`
}

type WalletUFVK struct {
	WalletID string
	UFVK     string
}

type BlockTip struct {
	Height int64
	Hash   string
}

type Block struct {
	Height   int64
	Hash     string
	PrevHash string
	Time     int64
}

type OrchardAction struct {
	Height          int64
	TxID            string
	ActionIndex     int32
	ActionNullifier string
	CMX             string
	EphemeralKey    string
	EncCiphertext   string
}

type OrchardActionHeight struct {
	Height  int64
	Actions []OrchardAction
}

type OrchardCommitment struct {
	Position    int64
	Height      int64
	TxID        string
	ActionIndex int32
	CMX         string
}

type OrchardSubtreeRoot struct {
	SubtreeIndex int64
	EndPosition  int64
	EndHeight    int64
	EndBlockHash string
	Root         string
}

type OrchardShardRoot struct {
	Version      int32
	ShardIndex   int64
	EndPosition  int64
	EndHeight    int64
	EndBlockHash string
	Root         string
}

type Note struct {
	WalletID                 string
	TxID                     string
	ActionIndex              int32
	Height                   int64
	Position                 *int64
	IsInternal               bool
	DiversifierIndex         uint32
	RecipientAddress         string
	ValueZat                 int64
	MemoHex                  *string
	NoteNullifier            string
	PendingSpentTxID         *string
	PendingSpentAt           *time.Time
	PendingSpentExpiryHeight *int64
	SpentHeight              *int64
	SpentTxID                *string
	ConfirmedHeight          *int64
	SpentConfirmedHeight     *int64
	CreatedAt                time.Time
}

type Event struct {
	ID        int64
	Kind      string
	WalletID  string
	Height    int64
	Payload   json.RawMessage
	CreatedAt time.Time
}

type EventFilter struct {
	BlockHeight *int64
	Kinds       []string
	TxID        string
}

type NotesCursor struct {
	Height      int64
	TxID        string
	ActionIndex int32
}

type NotesQuery struct {
	OnlyUnspent      bool
	MinValueZat      int64
	RecipientAddress string
	Limit            int
	Cursor           *NotesCursor
}

type NoteRef struct {
	TxID        string
	ActionIndex uint32
}

type WalletNoteStatusSnapshot struct {
	WalletFound       bool
	TipFound          bool
	EventEpoch        string
	AsOfScannerHeight int64
	AsOfScannerHash   string
	Notes             map[NoteRef]Note
}

func ValidateNoteStatusRefs(refs []NoteRef) error {
	if len(refs) < 1 || len(refs) > MaxNoteStatusBatch {
		return ErrInvalidNoteStatusQuery
	}
	seen := make(map[NoteRef]struct{}, len(refs))
	for _, ref := range refs {
		if len(ref.TxID) != 64 {
			return ErrInvalidNoteStatusQuery
		}
		for i := range ref.TxID {
			if (ref.TxID[i] < '0' || ref.TxID[i] > '9') && (ref.TxID[i] < 'a' || ref.TxID[i] > 'f') {
				return ErrInvalidNoteStatusQuery
			}
		}
		if _, ok := seen[ref]; ok {
			return ErrInvalidNoteStatusQuery
		}
		seen[ref] = struct{}{}
	}
	return nil
}

type AddressBalance struct {
	WalletFound        bool
	AvailableZat       int64
	PendingIncomingZat int64
	PendingOutgoingZat int64
	TotalUnspentZat    int64
}

type NoteValueSummary struct {
	NoteCount int64 `json:"note_count"`
	ValueZat  int64 `json:"value_zat"`
}

type SpendableNoteSummary struct {
	NoteValueSummary
	SmallestNoteZat *int64 `json:"smallest_note_zat"`
	LargestNoteZat  *int64 `json:"largest_note_zat"`
}

type PendingSpendNoteSummary struct {
	NoteValueSummary
	KnownExpiryCount int64  `json:"known_expiry_count"`
	NextExpiryHeight *int64 `json:"next_expiry_height"`
	LastExpiryHeight *int64 `json:"last_expiry_height"`
}

type WalletNoteSummary struct {
	WalletFound        bool
	TipFound           bool
	AsOfScannerHeight  int64
	AsOfScannerHash    string
	TotalUnspent       NoteValueSummary
	Spendable          SpendableNoteSummary
	Immature           NoteValueSummary
	PendingSpend       PendingSpendNoteSummary
	BelowMinNote       NoteValueSummary
	WitnessUnavailable NoteValueSummary
}

type OutgoingOutputsQuery struct {
	MinValueZat   int64
	Limit         int
	Cursor        *NotesCursor
	IncludeCursor bool
}

type OutgoingOutput struct {
	WalletID string

	TxID        string
	ActionIndex int32

	MinedHeight     *int64
	Position        *int64
	ConfirmedHeight *int64
	MempoolSeenAt   *time.Time
	TxExpiryHeight  *int64
	ExpiredAt       *time.Time

	RecipientAddress string
	ValueZat         int64
	MemoHex          *string

	OvkScope       string
	RecipientScope *string

	CreatedAt time.Time
}
