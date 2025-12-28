package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/postgres"
	"github.com/Abdullah1738/juno-scan/internal/store/rocksdb"
)

type Config struct {
	Driver string

	DSN    string
	Schema string
	Path   string
}

func Open(ctx context.Context, cfg Config) (store.Store, error) {
	driver := strings.ToLower(strings.TrimSpace(cfg.Driver))
	switch driver {
	case "", "postgres":
		return postgres.Open(ctx, cfg.DSN, cfg.Schema)
	case "rocksdb":
		if strings.TrimSpace(cfg.Path) == "" {
			return nil, errors.New("storage: db path is required for rocksdb")
		}
		return rocksdb.Open(cfg.Path)
	case "mysql":
		return openMySQL(ctx, cfg.DSN)
	default:
		return nil, fmt.Errorf("storage: unknown driver %q", cfg.Driver)
	}
}
