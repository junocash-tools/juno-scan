//go:build !mysql

package mysql

import (
	"context"
	"errors"
)

type Store struct{}

func Open(context.Context, string) (*Store, error) {
	return nil, errors.New("mysql adapter is not built; rebuild with -tags=mysql")
}
