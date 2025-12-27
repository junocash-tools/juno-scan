package migrate

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

func Apply(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return fmt.Errorf("migrate: db is nil")
	}

	if _, err := db.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migs {
		var already bool
		if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, m.version).Scan(&already); err != nil {
			return fmt.Errorf("migrate: check version %d: %w", m.version, err)
		}
		if already {
			continue
		}

		tx, err := db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("migrate: begin: %w", err)
		}
		_, execErr := tx.Exec(ctx, m.sql)
		if execErr == nil {
			_, execErr = tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.version)
		}
		if execErr != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate: apply %s: %w", m.name, execErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate: commit %s: %w", m.name, err)
		}
	}

	return nil
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate: readdir: %w", err)
	}

	var migs []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("migrate: invalid migration filename %q", name)
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("migrate: invalid migration version in %q", name)
		}
		b, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %q: %w", name, err)
		}
		migs = append(migs, migration{
			version: v,
			name:    name,
			sql:     string(b),
		})
	}

	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })
	return migs, nil
}
