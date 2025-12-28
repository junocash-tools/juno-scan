package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/api"
	"github.com/Abdullah1738/juno-scan/internal/config"
	"github.com/Abdullah1738/juno-scan/internal/db/migrate"
	"github.com/Abdullah1738/juno-scan/internal/scanner"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.FromFlags()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db := mustOpenDB(ctx, cfg)
	defer db.Close()

	if err := migrate.Apply(ctx, db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	rpc := sdkjunocashd.New(cfg.RPCURL, cfg.RPCUser, cfg.RPCPassword)
	sc, err := scanner.New(db, rpc, cfg.UAHRP, cfg.PollInterval)
	if err != nil {
		log.Fatalf("scanner init: %v", err)
	}

	go func() {
		if err := sc.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("scanner stopped: %v", err)
			cancel()
		}
	}()

	apiServer, err := api.New(db)
	if err != nil {
		log.Fatalf("api init: %v", err)
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s", cfg.ListenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
}

func mustOpenDB(ctx context.Context, cfg config.Config) *pgxpool.Pool {
	if strings.TrimSpace(cfg.DBSchema) == "" {
		pool, err := pgxpool.New(ctx, cfg.DBURL)
		if err != nil {
			log.Fatalf("db connect: %v", err)
		}
		return pool
	}

	schema := cfg.DBSchema

	adminConn, err := pgx.Connect(ctx, cfg.DBURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	if _, err := adminConn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
		_ = adminConn.Close(ctx)
		log.Fatalf("create schema: %v", err)
	}
	_ = adminConn.Close(ctx)

	poolCfg, err := pgxpool.ParseConfig(cfg.DBURL)
	if err != nil {
		log.Fatalf("db parse: %v", err)
	}
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	return pool
}
