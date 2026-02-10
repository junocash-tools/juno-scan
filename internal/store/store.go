package store

import (
	"context"
	"encoding/json"
	"time"
)

type Store interface {
	Close() error
	Migrate(ctx context.Context) error

	WithTx(ctx context.Context, fn func(Tx) error) error

	UpsertWallet(ctx context.Context, walletID, ufvk string) error
	ListWallets(ctx context.Context) ([]Wallet, error)
	ListEnabledWalletUFVKs(ctx context.Context) ([]WalletUFVK, error)

	Tip(ctx context.Context) (BlockTip, bool, error)
	HashAtHeight(ctx context.Context, height int64) (string, bool, error)
	RollbackToHeight(ctx context.Context, height int64) error

	WalletEventPublishCursor(ctx context.Context, walletID string) (int64, error)
	SetWalletEventPublishCursor(ctx context.Context, walletID string, cursor int64) error

	ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int, filter EventFilter) (events []Event, nextCursor int64, err error)
	ListWalletNotesPage(ctx context.Context, walletID string, query NotesQuery) (notes []Note, nextCursor *NotesCursor, err error)
	ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]Note, error)
	UpdatePendingSpends(ctx context.Context, pending map[string]PendingSpend, chainHeight int64, seenAt time.Time) error
	ListNotesByPendingSpentTxIDs(ctx context.Context, txids []string) ([]Note, error)
	ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]OrchardCommitment, error)
	FirstOrchardCommitmentPositionFromHeight(ctx context.Context, height int64) (pos int64, ok bool, err error)

	ListOutgoingOutputsByTxID(ctx context.Context, walletID, txid string) ([]OutgoingOutput, error)
}

type Tx interface {
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
}

type PendingSpend struct {
	TxID         string
	ExpiryHeight *int64
}

type Wallet struct {
	WalletID   string
	CreatedAt  time.Time
	DisabledAt *time.Time
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

type OrchardCommitment struct {
	Position    int64
	Height      int64
	TxID        string
	ActionIndex int32
	CMX         string
}

type Note struct {
	WalletID                 string
	TxID                     string
	ActionIndex              int32
	Height                   int64
	Position                 *int64
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
	OnlyUnspent bool
	MinValueZat int64
	Limit       int
	Cursor      *NotesCursor
}

type OutgoingOutput struct {
	WalletID string

	TxID        string
	ActionIndex int32

	MinedHeight     *int64
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
