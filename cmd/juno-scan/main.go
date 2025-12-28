package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/api"
	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/config"
	"github.com/Abdullah1738/juno-scan/internal/publisher"
	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/storage"
	"github.com/Abdullah1738/juno-scan/internal/store"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
)

func main() {
	cfg := config.FromFlags()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st := mustOpenStore(ctx, cfg)
	defer func() { _ = st.Close() }()

	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	br, err := broker.Open(ctx, broker.Config{
		Driver: cfg.BrokerDriver,
		URL:    cfg.BrokerURL,
		Topic:  cfg.BrokerTopic,
	})
	if err != nil {
		log.Fatalf("broker init: %v", err)
	}
	defer func() {
		if br != nil {
			_ = br.Close()
		}
	}()

	if br != nil {
		pub, err := publisher.New(st, br, publisher.Config{
			PollInterval: cfg.BrokerPollInterval,
			BatchSize:    cfg.BrokerBatchSize,
		})
		if err != nil {
			log.Fatalf("publisher init: %v", err)
		}

		go func() {
			if err := pub.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("publisher stopped: %v", err)
				cancel()
			}
		}()
	}

	rpc := sdkjunocashd.New(cfg.RPCURL, cfg.RPCUser, cfg.RPCPassword)
	sc, err := scanner.New(st, rpc, cfg.UAHRP, cfg.PollInterval, cfg.Confirmations)
	if err != nil {
		log.Fatalf("scanner init: %v", err)
	}

	go func() {
		if err := sc.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("scanner stopped: %v", err)
			cancel()
		}
	}()

	apiServer, err := api.New(st)
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

func mustOpenStore(ctx context.Context, cfg config.Config) store.Store {
	s, err := storage.Open(ctx, storage.Config{
		Driver: cfg.DBDriver,
		DSN:    cfg.DBDSN,
		Schema: cfg.DBSchema,
		Path:   cfg.DBPath,
	})
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	return s
}
