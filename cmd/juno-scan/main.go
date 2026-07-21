package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/api"
	"github.com/Abdullah1738/juno-scan/internal/backfill"
	"github.com/Abdullah1738/juno-scan/internal/broker"
	"github.com/Abdullah1738/juno-scan/internal/config"
	"github.com/Abdullah1738/juno-scan/internal/publisher"
	"github.com/Abdullah1738/juno-scan/internal/scanner"
	"github.com/Abdullah1738/juno-scan/internal/shardcache"
	"github.com/Abdullah1738/juno-scan/internal/storage"
	"github.com/Abdullah1738/juno-scan/internal/store"
	sdkjunocashd "github.com/Abdullah1738/juno-sdk-go/junocashd"
	"golang.org/x/sync/errgroup"
)

func main() {
	cfg := config.FromFlags()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	network, err := config.ResolveNetwork(cfg.Network, cfg.UAHRP)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.UAHRP) == "" {
		cfg.UAHRP, err = config.ResolveUAHRP(network)
		if err != nil {
			return err
		}
	}

	st := mustOpenStore(runCtx, cfg)
	defer func() { _ = st.Close() }()

	if err := st.Migrate(runCtx); err != nil {
		return err
	}

	br, err := broker.Open(runCtx, broker.Config{
		Driver: cfg.BrokerDriver,
		URL:    cfg.BrokerURL,
		Topic:  cfg.BrokerTopic,
	})
	if err != nil {
		return err
	}
	defer func() {
		if br != nil {
			_ = br.Close()
		}
	}()

	g, groupCtx := errgroup.WithContext(runCtx)

	if br != nil {
		pub, err := publisher.New(st, br, publisher.Config{
			PollInterval: cfg.BrokerPollInterval,
			BatchSize:    cfg.BrokerBatchSize,
		})
		if err != nil {
			return err
		}

		g.Go(func() error {
			if err := pub.Run(groupCtx); err != nil && !errors.Is(err, context.Canceled) && groupCtx.Err() == nil {
				return err
			}
			return nil
		})
	}

	rpc := sdkjunocashd.New(cfg.RPCURL, cfg.RPCUser, cfg.RPCPassword)
	sc, err := scanner.New(st, rpc, cfg.UAHRP, cfg.PollInterval, cfg.Confirmations, cfg.ZMQHashBlock)
	if err != nil {
		return err
	}

	g.Go(func() error {
		if err := sc.Run(groupCtx); err != nil && !errors.Is(err, context.Canceled) && groupCtx.Err() == nil {
			return err
		}
		return nil
	})

	var shardCache *shardcache.Service
	if shardStore, ok := st.(shardcache.Store); ok {
		shardCache, err = shardcache.New(shardStore, shardcache.Options{
			Enabled: cfg.ShardCacheEnabled, BatchSize: cfg.ShardCacheBatch,
			PollInterval: cfg.ShardCachePoll, Yield: cfg.ShardCacheYield,
			NodeHeight: func() (int64, bool) {
				status := sc.Status()
				return status.NodeHeight, status.NodeHeightKnown
			},
		})
		if err != nil {
			return err
		}
		if cfg.ShardCacheEnabled {
			g.Go(func() error {
				if err := shardCache.Run(groupCtx); err != nil && !errors.Is(err, context.Canceled) && groupCtx.Err() == nil {
					return err
				}
				return nil
			})
		}
	} else if cfg.ShardCacheEnabled {
		return errors.New("configured store does not support Orchard shard cache")
	}

	bf, err := backfill.New(st, rpc, cfg.UAHRP, cfg.Confirmations)
	if err != nil {
		return err
	}

	apiServer, err := api.New(st,
		api.WithBackfillService(bf),
		api.WithBearerToken(cfg.APIBearerToken),
		api.WithRuntimeStatus(network, cfg.UAHRP, cfg.Confirmations, cfg.MaxReadyLag, func() (int64, bool) {
			status := sc.Status()
			return status.NodeHeight, status.NodeHeightKnown
		}),
		api.WithShardCacheService(shardCache),
		api.WithWitnessMode(cfg.WitnessMode),
	)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-groupCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s", cfg.ListenAddr)
	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
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
