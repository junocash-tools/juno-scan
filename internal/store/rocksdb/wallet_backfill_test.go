package rocksdb

import (
	"context"
	"errors"
	"testing"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func TestWalletBirthdayInitializesAndResetsBackfillProgress(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/scan.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 100); err != nil {
		t.Fatal(err)
	}
	p, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || p.BirthdayHeight != 100 || p.NextHeight != 100 || p.State != "pending" {
		t.Fatalf("initial progress=%+v ok=%v err=%v", p, ok, err)
	}
	p.NextHeight, p.TargetHeight, p.State = 201, 200, "complete"
	if err := st.SetWalletBackfillProgress(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 100); err != nil {
		t.Fatal(err)
	}
	p, _, _ = st.WalletBackfillStatus(ctx, "exchange")
	if p.NextHeight != 201 || p.State != "complete" {
		t.Fatalf("same birthday should preserve progress: %+v", p)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "changed", 100); !errors.Is(err, store.ErrWalletUFVKMismatch) {
		t.Fatalf("changed UFVK err=%v", err)
	}
	if err := st.UpsertWalletBirthday(ctx, "other", "ufvk", 0); !errors.Is(err, store.ErrUFVKAlreadyRegistered) {
		t.Fatalf("duplicate UFVK err=%v", err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 101); !errors.Is(err, store.ErrBirthdayIncrease) {
		t.Fatalf("birthday increase err=%v", err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	p, _, _ = st.WalletBackfillStatus(ctx, "exchange")
	if p.BirthdayHeight != 50 || p.NextHeight != 50 || p.State != "pending" {
		t.Fatalf("changed birthday should reset progress: %+v", p)
	}
}
