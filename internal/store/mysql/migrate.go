//go:build mysql

package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("mysql: db is nil")
	}

	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INT PRIMARY KEY,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);`); err != nil {
		return fmt.Errorf("mysql: create schema_migrations: %w", err)
	}

	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migs {
		var already bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=?)`, m.version).Scan(&already); err != nil {
			return fmt.Errorf("mysql: check version %d: %w", m.version, err)
		}
		if already {
			continue
		}

		if _, err := db.ExecContext(ctx, m.sql); err != nil {
			return fmt.Errorf("mysql: apply %s: %w", m.name, err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, m.version); err != nil {
			return fmt.Errorf("mysql: record %s: %w", m.name, err)
		}
	}

	return nil
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("mysql: readdir: %w", err)
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
			return nil, fmt.Errorf("mysql: invalid migration filename %q", name)
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("mysql: invalid migration version in %q", name)
		}
		b, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("mysql: read %q: %w", name, err)
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
