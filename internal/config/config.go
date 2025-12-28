package config

import (
	"flag"
	"os"
	"strconv"
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

	ListenAddr    string
	UAHRP         string
	PollInterval  time.Duration
	Confirmations int64
	ZMQHashBlock  string

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
	flag.StringVar(&cfg.UAHRP, "ua-hrp", getenv("JUNO_SCAN_UA_HRP", "j"), "Unified address HRP (e.g. j, jregtest)")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", getenvDuration("JUNO_SCAN_POLL_INTERVAL", 2*time.Second), "Poll interval for new blocks (when ZMQ is not used)")
	flag.Int64Var(&cfg.Confirmations, "confirmations", getenvInt64("JUNO_SCAN_CONFIRMATIONS", 100), "Confirmations required for DepositConfirmed event")
	flag.StringVar(&cfg.ZMQHashBlock, "zmq-hashblock", getenv("JUNO_SCAN_ZMQ_HASHBLOCK", ""), "Optional ZMQ endpoint for hashblock notifications (tcp://host:port)")

	flag.StringVar(&cfg.BrokerDriver, "broker-driver", getenv("JUNO_SCAN_BROKER_DRIVER", "none"), "Message broker driver (none, kafka, nats, rabbitmq)")
	flag.StringVar(&cfg.BrokerURL, "broker-url", getenv("JUNO_SCAN_BROKER_URL", ""), "Message broker URL/DSN")
	flag.StringVar(&cfg.BrokerTopic, "broker-topic", getenv("JUNO_SCAN_BROKER_TOPIC", "juno.scan.events"), "Message broker topic/subject/queue name")
	flag.DurationVar(&cfg.BrokerPollInterval, "broker-poll-interval", getenvDuration("JUNO_SCAN_BROKER_POLL_INTERVAL", 500*time.Millisecond), "Broker outbox poll interval")
	flag.IntVar(&cfg.BrokerBatchSize, "broker-batch-size", getenvInt("JUNO_SCAN_BROKER_BATCH_SIZE", 1000), "Broker outbox batch size")

	flag.Parse()

	if dsn == "" {
		dsn = legacyURL
	}
	cfg.DBDSN = dsn
	return cfg
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
