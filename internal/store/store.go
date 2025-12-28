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

	ListWalletEvents(ctx context.Context, walletID string, afterID int64, limit int) (events []Event, nextCursor int64, err error)
	ListWalletNotes(ctx context.Context, walletID string, onlyUnspent bool, limit int) ([]Note, error)
	ListOrchardCommitmentsUpToHeight(ctx context.Context, height int64) ([]OrchardCommitment, error)
}

type Tx interface {
	InsertBlock(ctx context.Context, b Block) error
	NextOrchardCommitmentPosition(ctx context.Context) (int64, error)
	InsertOrchardAction(ctx context.Context, a OrchardAction) error
	InsertOrchardCommitment(ctx context.Context, c OrchardCommitment) error
	MarkNotesSpent(ctx context.Context, height int64, txid string, nullifiers []string) ([]Note, error)
	ConfirmNotes(ctx context.Context, confirmationHeight int64, maxNoteHeight int64) ([]Note, error)
	ConfirmSpends(ctx context.Context, confirmationHeight int64, maxSpentHeight int64) ([]Note, error)
	InsertNote(ctx context.Context, n Note) error
	InsertEvent(ctx context.Context, e Event) error
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
	WalletID             string
	TxID                 string
	ActionIndex          int32
	Height               int64
	Position             *int64
	DiversifierIndex     uint32
	RecipientAddress     string
	ValueZat             int64
	MemoHex              *string
	NoteNullifier        string
	SpentHeight          *int64
	SpentTxID            *string
	ConfirmedHeight      *int64
	SpentConfirmedHeight *int64
	CreatedAt            time.Time
}

type Event struct {
	ID        int64
	Kind      string
	WalletID  string
	Height    int64
	Payload   json.RawMessage
	CreatedAt time.Time
}
