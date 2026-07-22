package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

func TestHTTPServerBoundsRequestReads(t *testing.T) {
	srv := newHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout != 5*time.Second || srv.ReadTimeout != 15*time.Second {
		t.Fatalf("read_header_timeout=%v read_timeout=%v", srv.ReadHeaderTimeout, srv.ReadTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Fatalf("write_timeout=%v want unbounded for long backfills", srv.WriteTimeout)
	}
}

type startupStoreRecorder struct {
	migrations int
	rotations  int
	migrateErr error
	rotateErr  error
}

func TestPrepareStoreForStartupRotatesRealRocksEpochAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/startup.db"
	st, err := rocksdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareStoreForStartup(ctx, st); err != nil {
		t.Fatal(err)
	}
	first, err := st.EventEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = rocksdb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := prepareStoreForStartup(ctx, st); err != nil {
		t.Fatal(err)
	}
	second, err := st.EventEpoch(ctx)
	if err != nil || second == first {
		t.Fatalf("restart epoch first=%q second=%q err=%v", first, second, err)
	}
}

func (s *startupStoreRecorder) Migrate(context.Context) error {
	s.migrations++
	return s.migrateErr
}

func (s *startupStoreRecorder) RotateEventEpoch(context.Context) (string, error) {
	s.rotations++
	return "epoch", s.rotateErr
}

func TestPrepareStoreForStartupRotatesExactlyOnceAfterMigration(t *testing.T) {
	recorder := &startupStoreRecorder{}
	if err := prepareStoreForStartup(context.Background(), recorder); err != nil {
		t.Fatal(err)
	}
	if recorder.migrations != 1 || recorder.rotations != 1 {
		t.Fatalf("migrations=%d rotations=%d", recorder.migrations, recorder.rotations)
	}
	recorder = &startupStoreRecorder{migrateErr: errors.New("migration failed")}
	if err := prepareStoreForStartup(context.Background(), recorder); err == nil {
		t.Fatal("expected migration error")
	}
	if recorder.rotations != 0 {
		t.Fatalf("epoch rotated after failed migration: %d", recorder.rotations)
	}
}
