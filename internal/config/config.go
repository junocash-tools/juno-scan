package config

import (
	"errors"
	"flag"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DBDriver string
	DBDSN    string
	DBSchema string
	DBPath   string

	RPCURL      string
	RPCUser     string
	RPCPassword string

	ListenAddr        string
	APIBearerToken    string
	UAHRP             string
	Network           string
	PollInterval      time.Duration
	Confirmations     int64
	MaxReadyLag       int64
	ZMQHashBlock      string
	ShardCacheEnabled bool
	ShardCacheBatch   int
	ShardCachePoll    time.Duration
	ShardCacheYield   time.Duration
	WitnessMode       string

	BrokerDriver       string
	BrokerURL          string
	BrokerTopic        string
	BrokerPollInterval time.Duration
	BrokerBatchSize    int
}

func FromFlags() Config {
	var cfg Config

	flag.StringVar(&cfg.DBDriver, "db-driver", getenv("JUNO_SCAN_DB_DRIVER", "postgres"), "Database driver (postgres, mysql, rocksdb)")

	var dsn string
	var legacyURL string
	flag.StringVar(&dsn, "db-dsn", getenv("JUNO_SCAN_DB_DSN", ""), "Database DSN for postgres/mysql")
	flag.StringVar(&legacyURL, "db-url", getenv("JUNO_SCAN_DB_URL", "postgres://localhost:5432/junoscan?sslmode=disable"), "Deprecated alias for -db-dsn")

	flag.StringVar(&cfg.DBSchema, "db-schema", getenv("JUNO_SCAN_DB_SCHEMA", ""), "Postgres schema for juno-scan tables (optional)")
	flag.StringVar(&cfg.DBPath, "db-path", getenv("JUNO_SCAN_DB_PATH", ""), "RocksDB (Pebble) path (required when db-driver=rocksdb)")

	flag.StringVar(&cfg.RPCURL, "rpc-url", getenv("JUNO_SCAN_RPC_URL", "http://127.0.0.1:8232"), "junocashd RPC URL")
	flag.StringVar(&cfg.RPCUser, "rpc-user", getenv("JUNO_SCAN_RPC_USER", ""), "junocashd RPC username")
	flag.StringVar(&cfg.RPCPassword, "rpc-pass", getenv("JUNO_SCAN_RPC_PASS", ""), "junocashd RPC password")

	flag.StringVar(&cfg.ListenAddr, "listen", getenv("JUNO_SCAN_LISTEN", "127.0.0.1:8080"), "HTTP listen address")
	flag.StringVar(&cfg.APIBearerToken, "api-bearer-token", getenv("JUNO_SCAN_API_BEARER_TOKEN", ""), "Optional bearer token for HTTP API requests (Authorization: Bearer ...)")
	flag.StringVar(&cfg.UAHRP, "ua-hrp", getenv("JUNO_SCAN_UA_HRP", ""), "Unified address HRP (defaults from network: j, jtest, jregtest)")
	flag.StringVar(&cfg.Network, "network", getenv("JUNO_SCAN_NETWORK", getenv("JUNO_NETWORK", "auto")), "Network identity (auto, mainnet, testnet, regtest)")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", getenvDuration("JUNO_SCAN_POLL_INTERVAL", 2*time.Second), "Poll interval for new blocks (when ZMQ is not used)")
	flag.Int64Var(&cfg.Confirmations, "confirmations", getenvInt64("JUNO_SCAN_CONFIRMATIONS", 100), "Confirmations required for DepositConfirmed event")
	flag.Int64Var(&cfg.MaxReadyLag, "max-ready-lag", getenvInt64("JUNO_SCAN_MAX_READY_LAG", 2), "Maximum node-to-scanner lag reported as ready")
	flag.StringVar(&cfg.ZMQHashBlock, "zmq-hashblock", getenv("JUNO_SCAN_ZMQ_HASHBLOCK", ""), "Optional ZMQ endpoint for hashblock notifications (tcp://host:port)")
	flag.BoolVar(&cfg.ShardCacheEnabled, "shard-cache-enabled", getenvBool("JUNO_SCAN_SHARD_CACHE_ENABLED", true), "Enable level-12 Orchard shard-root cache backfill")
	flag.IntVar(&cfg.ShardCacheBatch, "shard-cache-batch", getenvInt("JUNO_SCAN_SHARD_CACHE_BATCH", 1), "Maximum shard roots computed per background pass")
	flag.DurationVar(&cfg.ShardCachePoll, "shard-cache-poll", getenvDuration("JUNO_SCAN_SHARD_CACHE_POLL", 5*time.Second), "Shard-root cache background poll interval")
	flag.DurationVar(&cfg.ShardCacheYield, "shard-cache-yield", getenvDuration("JUNO_SCAN_SHARD_CACHE_YIELD", 25*time.Millisecond), "Yield between shard-root computations")
	flag.StringVar(&cfg.WitnessMode, "witness-mode", getenv("JUNO_SCAN_WITNESS_MODE", "auto"), "Witness compute mode (auto, shard, subtree, legacy)")

	flag.StringVar(&cfg.BrokerDriver, "broker-driver", getenv("JUNO_SCAN_BROKER_DRIVER", "none"), "Message broker driver (none, kafka, nats, rabbitmq)")
	flag.StringVar(&cfg.BrokerURL, "broker-url", getenv("JUNO_SCAN_BROKER_URL", ""), "Message broker URL/DSN")
	flag.StringVar(&cfg.BrokerTopic, "broker-topic", getenv("JUNO_SCAN_BROKER_TOPIC", "juno.scan.events"), "Message broker topic/subject/queue name")
	flag.DurationVar(&cfg.BrokerPollInterval, "broker-poll-interval", getenvDuration("JUNO_SCAN_BROKER_POLL_INTERVAL", 500*time.Millisecond), "Broker outbox poll interval")
	flag.IntVar(&cfg.BrokerBatchSize, "broker-batch-size", getenvInt("JUNO_SCAN_BROKER_BATCH_SIZE", 1000), "Broker outbox batch size")

	flag.Parse()
	if strings.TrimSpace(cfg.UAHRP) == "" {
		cfg.UAHRP, _ = ResolveUAHRP(cfg.Network)
	}

	if dsn == "" {
		dsn = legacyURL
	}
	cfg.DBDSN = dsn
	return cfg
}

func ResolveUAHRP(network string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "", "auto", "mainnet":
		return "j", nil
	case "testnet":
		return "jtest", nil
	case "regtest":
		return "jregtest", nil
	default:
		return "", errors.New("unsupported network")
	}
}

func ResolveNetwork(network, uaHRP string) (string, error) {
	network = strings.ToLower(strings.TrimSpace(network))
	uaHRP = strings.ToLower(strings.TrimSpace(uaHRP))
	if uaHRP == "" {
		resolved, err := ResolveUAHRP(network)
		if err != nil {
			return "", err
		}
		uaHRP = resolved
	}
	derived := ""
	switch uaHRP {
	case "j":
		derived = "mainnet"
	case "jtest":
		derived = "testnet"
	case "jregtest":
		derived = "regtest"
	default:
		return "", errors.New("unsupported unified address HRP")
	}
	if network == "" || network == "auto" {
		return derived, nil
	}
	if network != "mainnet" && network != "testnet" && network != "regtest" {
		return "", errors.New("unsupported network")
	}
	if network != derived {
		return "", errors.New("network and unified address HRP mismatch")
	}
	return network, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
