//go:build !mysql

package storage

import (
	"context"
	"errors"

	"github.com/Abdullah1738/juno-scan/internal/store"
)

func openMySQL(context.Context, string) (store.Store, error) {
	return nil, errors.New("storage: mysql adapter is not built; rebuild with -tags=mysql")
}
