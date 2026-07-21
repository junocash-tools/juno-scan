package rocksdb

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/cockroachdb/pebble"
)

func TestV4MigrationRepairsWalletProgressBirthdayMismatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/mismatch-migrate.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	corrupt, _ := json.Marshal(walletBackfillRecord{BirthdayHeight: 100, NextHeight: 201, TargetHeight: 200, State: "complete", Generation: 7, UpdatedAtUnix: time.Now().Unix()})
	if err := st.db.Set(keyWalletBackfill("exchange"), corrupt, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 50 || progress.NextHeight != 50 || progress.TargetHeight != 0 || progress.State != "pending" || progress.Generation != 8 {
		t.Fatalf("mismatch not repaired: %+v ok=%v err=%v", progress, ok, err)
	}
}

func TestUpsertWalletRepairsProgressBirthdayMismatch(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/mismatch-upsert.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	corrupt, _ := json.Marshal(walletBackfillRecord{BirthdayHeight: 100, NextHeight: 201, TargetHeight: 200, State: "complete", Generation: 3, UpdatedAtUnix: time.Now().Unix()})
	if err := st.db.Set(keyWalletBackfill("exchange"), corrupt, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertWallet(ctx, "exchange", "ufvk"); err != nil {
		t.Fatal(err)
	}
	progress, ok, err := st.WalletBackfillStatus(ctx, "exchange")
	if err != nil || !ok || progress.BirthdayHeight != 50 || progress.NextHeight != 50 || progress.TargetHeight != 0 || progress.State != "pending" || progress.Generation != 4 {
		t.Fatalf("mismatch not repaired: %+v ok=%v err=%v", progress, ok, err)
	}
}

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
	if err != nil || !ok || p.BirthdayHeight != 100 || p.NextHeight != 100 || p.State != "pending" || p.Generation < 1 {
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
	if p.BirthdayHeight != 50 || p.NextHeight != 50 || p.State != "pending" || p.Generation < 2 {
		t.Fatalf("changed birthday should reset progress: %+v", p)
	}
}

func TestWalletBackfillProgressRejectsStaleGenerationAndNextHeight(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir() + "/cas.db")
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
	stale, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	first := stale
	expected := first.NextHeight
	first.ExpectedNextHeight = &expected
	first.NextHeight = 101
	if err := st.SetWalletBackfillProgress(ctx, first); err != nil {
		t.Fatal(err)
	}
	stale.ExpectedNextHeight = &expected
	stale.NextHeight = 102
	if err := st.SetWalletBackfillProgress(ctx, stale); !errors.Is(err, store.ErrBackfillProgressConflict) {
		t.Fatalf("stale next-height CAS err=%v", err)
	}
	beforeRewind, _, _ := st.WalletBackfillStatus(ctx, "exchange")
	if err := st.UpsertWalletBirthday(ctx, "exchange", "ufvk", 50); err != nil {
		t.Fatal(err)
	}
	beforeRewind.NextHeight = 200
	if err := st.SetWalletBackfillProgress(ctx, beforeRewind); !errors.Is(err, store.ErrBackfillProgressConflict) {
		t.Fatalf("stale generation err=%v", err)
	}
}
