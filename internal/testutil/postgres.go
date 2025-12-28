package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TestPostgres struct {
	Pool    *pgxpool.Pool
	Schema  string
	BaseURL string
}

func OpenTestPostgres(ctx context.Context, baseURL string) (*TestPostgres, error) {
	suffix, err := randHex(8)
	if err != nil {
		return nil, err
	}
	schema := "junoscan_test_" + suffix

	adminConn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	defer adminConn.Close(ctx)

	if _, err := adminConn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}

	cfg, err := pgxpool.ParseConfig(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = adminConn.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		return nil, fmt.Errorf("postgres pool: %w", err)
	}

	return &TestPostgres{
		Pool:    pool,
		Schema:  schema,
		BaseURL: baseURL,
	}, nil
}

func (t *TestPostgres) Close(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.Pool != nil {
		t.Pool.Close()
	}
	adminConn, err := pgx.Connect(ctx, t.BaseURL)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}
	defer adminConn.Close(ctx)

	if _, err := adminConn.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, t.Schema)); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}
	return nil
}

func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
