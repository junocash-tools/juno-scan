package api

import (
	"context"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func currentMempoolRefreshOption(t *testing.T, st store.Store) Option {
	t.Helper()
	status := currentMempoolRefreshStatus(t, st)
	return WithMempoolRefreshStatus(func() MempoolRefreshStatus { return status })
}

func liveMempoolRefreshOption(st store.Store) Option {
	return WithMempoolRefreshStatus(func() MempoolRefreshStatus {
		eventEpoch, epochErr := st.EventEpoch(context.Background())
		tip, found, tipErr := st.Tip(context.Background())
		if epochErr != nil || tipErr != nil || !found {
			return MempoolRefreshStatus{}
		}
		return MempoolRefreshStatus{
			Ready:      true,
			EventEpoch: eventEpoch,
			Height:     tip.Height,
			Hash:       tip.Hash,
		}
	})
}

func currentMempoolRefreshStatus(t *testing.T, st store.Store) MempoolRefreshStatus {
	t.Helper()
	eventEpoch, err := st.EventEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tip, found, err := st.Tip(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return MempoolRefreshStatus{
		Ready:      found,
		EventEpoch: eventEpoch,
		Height:     tip.Height,
		Hash:       tip.Hash,
	}
}
