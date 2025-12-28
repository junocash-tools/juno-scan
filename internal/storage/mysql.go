//go:build mysql

package storage

import (
	"context"

	"github.com/Abdullah1738/juno-scan/internal/store"
	"github.com/Abdullah1738/juno-scan/internal/store/mysql"
)

func openMySQL(ctx context.Context, dsn string) (store.Store, error) {
	return mysql.Open(ctx, dsn)
}
