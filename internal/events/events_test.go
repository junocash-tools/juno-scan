package events

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Abdullah1738/juno-sdk-go/types"
)

func depositPayload(index uint32) DepositEventPayload {
	return DepositEventPayload{
		DepositEvent: DepositEvent{
			Version:          types.V1,
			WalletID:         "exchange",
			DiversifierIndex: index,
			TxID:             "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Height:           1,
			ActionIndex:      0,
			AmountZatoshis:   1,
			Status: types.TxStatus{
				State:         types.TxStateConfirmed,
				Height:        1,
				Confirmations: 1,
			},
		},
		Origin:           string(types.DepositOriginExternal),
		RecipientAddress: "jregtest1recipient",
	}
}

func TestDepositLifecyclePayloadsAlwaysIncludeDiversifierIndex(t *testing.T) {
	for _, index := range []uint32{0, 7} {
		base := depositPayload(index)
		for _, test := range []struct {
			name     string
			payload  any
			required []string
		}{
			{name: "detected", payload: base},
			{name: "confirmed", payload: DepositConfirmedPayload{DepositEventPayload: base, ConfirmedHeight: 100, RequiredConfirmations: 100}, required: []string{"confirmed_height", "required_confirmations"}},
			{name: "unconfirmed", payload: DepositUnconfirmedPayload{DepositEventPayload: base, RollbackHeight: 99, PreviousConfirmedHeight: 100}, required: []string{"rollback_height", "previous_confirmed_height"}},
			{name: "orphaned", payload: DepositOrphanedPayload{DepositEventPayload: base, OrphanedAtHeight: 0}, required: []string{"orphaned_at_height"}},
		} {
			t.Run(strconv.FormatUint(uint64(index), 10)+"/"+test.name, func(t *testing.T) {
				encoded, err := json.Marshal(test.payload)
				if err != nil {
					t.Fatalf("marshal deposit payload: %v", err)
				}
				var decoded map[string]json.RawMessage
				if err := json.Unmarshal(encoded, &decoded); err != nil {
					t.Fatalf("decode deposit payload: %v", err)
				}
				value, ok := decoded["diversifier_index"]
				if !ok {
					t.Fatalf("diversifier_index was omitted for address index %d", index)
				}
				if string(value) != strconv.FormatUint(uint64(index), 10) {
					t.Fatalf("diversifier_index = %s, want %d", value, index)
				}
				for _, field := range test.required {
					if _, ok := decoded[field]; !ok {
						t.Fatalf("%s was omitted from %s deposit payload", field, test.name)
					}
				}
			})
		}
	}
}
