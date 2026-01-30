package events

import "github.com/Abdullah1738/juno-sdk-go/types"

const (
	KindDepositEvent       = "DepositEvent"
	KindDepositConfirmed   = "DepositConfirmed"
	KindDepositOrphaned    = "DepositOrphaned"
	KindDepositUnconfirmed = "DepositUnconfirmed"

	KindSpendEvent       = "SpendEvent"
	KindSpendConfirmed   = "SpendConfirmed"
	KindSpendOrphaned    = "SpendOrphaned"
	KindSpendUnconfirmed = "SpendUnconfirmed"

	KindOutgoingOutputEvent       = "OutgoingOutputEvent"
	KindOutgoingOutputConfirmed   = "OutgoingOutputConfirmed"
	KindOutgoingOutputOrphaned    = "OutgoingOutputOrphaned"
	KindOutgoingOutputUnconfirmed = "OutgoingOutputUnconfirmed"
)

type DepositEventPayload struct {
	types.DepositEvent
	RecipientAddress string `json:"recipient_address,omitempty"`
	NoteNullifier    string `json:"note_nullifier,omitempty"`
}

type DepositConfirmedPayload struct {
	DepositEventPayload
	ConfirmedHeight       int64 `json:"confirmed_height"`
	RequiredConfirmations int64 `json:"required_confirmations"`
}

type DepositOrphanedPayload struct {
	DepositEventPayload
	OrphanedAtHeight int64 `json:"orphaned_at_height"`
}

type DepositUnconfirmedPayload struct {
	DepositEventPayload
	RollbackHeight          int64 `json:"rollback_height"`
	RequiredConfirmations   int64 `json:"required_confirmations,omitempty"`
	PreviousConfirmedHeight int64 `json:"previous_confirmed_height"`
}

type SpendEventPayload struct {
	Version          types.Version `json:"version"`
	WalletID         string        `json:"wallet_id"`
	DiversifierIndex uint32        `json:"diversifier_index,omitempty"`
	TxID             string        `json:"txid"`
	Height           int64         `json:"height"`

	NoteTxID        string `json:"note_txid"`
	NoteActionIndex uint32 `json:"note_action_index"`
	NoteHeight      int64  `json:"note_height"`
	AmountZatoshis  uint64 `json:"amount_zatoshis"`
	NoteNullifier   string `json:"note_nullifier,omitempty"`

	RecipientAddress string         `json:"recipient_address,omitempty"`
	Status           types.TxStatus `json:"status"`
}

type SpendConfirmedPayload struct {
	SpendEventPayload
	ConfirmedHeight       int64 `json:"confirmed_height"`
	RequiredConfirmations int64 `json:"required_confirmations"`
}

type SpendOrphanedPayload struct {
	SpendEventPayload
	OrphanedAtHeight int64 `json:"orphaned_at_height"`
}

type SpendUnconfirmedPayload struct {
	SpendEventPayload
	RollbackHeight          int64 `json:"rollback_height"`
	RequiredConfirmations   int64 `json:"required_confirmations,omitempty"`
	PreviousConfirmedHeight int64 `json:"previous_confirmed_height"`
}

type OutgoingOutputEventPayload struct {
	Version  types.Version `json:"version"`
	WalletID string        `json:"wallet_id"`

	TxID  string `json:"txid"`
	Height *int64 `json:"height,omitempty"`

	ActionIndex    uint32 `json:"action_index"`
	AmountZatoshis uint64 `json:"amount_zatoshis"`

	RecipientAddress string `json:"recipient_address"`
	MemoHex          string `json:"memo_hex,omitempty"`

	OvkScope       string `json:"ovk_scope"`
	RecipientScope string `json:"recipient_scope,omitempty"`

	Status types.TxStatus `json:"status"`
}

type OutgoingOutputConfirmedPayload struct {
	OutgoingOutputEventPayload
	ConfirmedHeight       int64 `json:"confirmed_height"`
	RequiredConfirmations int64 `json:"required_confirmations"`
}

type OutgoingOutputOrphanedPayload struct {
	OutgoingOutputEventPayload
	OrphanedAtHeight int64 `json:"orphaned_at_height"`
}

type OutgoingOutputUnconfirmedPayload struct {
	OutgoingOutputEventPayload
	RollbackHeight          int64 `json:"rollback_height"`
	RequiredConfirmations   int64 `json:"required_confirmations,omitempty"`
	PreviousConfirmedHeight int64 `json:"previous_confirmed_height"`
}
