//go:build docker

package containers

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultJunocashVersion  = "0.9.7"
	defaultPostgresImage    = "postgres:16-alpine"
	defaultMySQLImage       = "mysql:8.4"
	defaultNATSImage        = "nats:2.10-alpine"
	defaultRabbitMQImage    = "rabbitmq:3.13-management-alpine"
	defaultKafkaImage       = "bitnami/kafka:3.7"
	defaultRPCUser          = "rpcuser"
	defaultRPCPassword      = "rpcpass"
	defaultPostgresUser     = "junoscan"
	defaultPostgresPassword = "junoscan"
	defaultPostgresDB       = "junoscan"
	defaultMySQLRootPass    = "root"
	defaultMySQLDB          = "junoscan"
	defaultMySQLUser        = "junoscan"
	defaultMySQLPassword    = "junoscan"
)

type IntegrationStack struct {
	JunocashdContainerID string
	RPCURL               string
	RPCUser              string
	RPCPassword          string
	ZMQHashBlockEndpoint string

	PostgresDSN  string
	MySQLRootDSN string

	NATSURL      string
	RabbitMQURL  string
	KafkaBrokers string

	junocashd testcontainers.Container
	postgres  testcontainers.Container
	mysql     testcontainers.Container
	nats      testcontainers.Container
	rabbitmq  testcontainers.Container
	kafka     testcontainers.Container
}

func StartIntegrationStack(ctx context.Context) (*IntegrationStack, error) {
	st := &IntegrationStack{
		RPCUser:     defaultRPCUser,
		RPCPassword: defaultRPCPassword,
	}

	cleanup := func() {
		_ = st.Terminate(context.Background())
	}

	var err error
	st.postgres, st.PostgresDSN, err = startPostgres(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	st.mysql, st.MySQLRootDSN, err = startMySQL(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	st.nats, st.NATSURL, err = startNATS(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	st.rabbitmq, st.RabbitMQURL, err = startRabbitMQ(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	st.kafka, st.KafkaBrokers, err = startKafka(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}

	st.junocashd, st.RPCURL, st.ZMQHashBlockEndpoint, st.JunocashdContainerID, err = startJunocashd(ctx, st.RPCUser, st.RPCPassword)
	if err != nil {
		cleanup()
		return nil, err
	}

	return st, nil
}

type Junocashd struct {
	ContainerID          string
	RPCURL               string
	RPCUser              string
	RPCPassword          string
	ZMQHashBlockEndpoint string

	c testcontainers.Container
}

func StartJunocashd(ctx context.Context) (*Junocashd, error) {
	rpcUser := defaultRPCUser
	rpcPass := defaultRPCPassword

	c, rpcURL, zmqEndpoint, id, err := startJunocashd(ctx, rpcUser, rpcPass)
	if err != nil {
		return nil, err
	}

	return &Junocashd{
		ContainerID:          id,
		RPCURL:               rpcURL,
		RPCUser:              rpcUser,
		RPCPassword:          rpcPass,
		ZMQHashBlockEndpoint: zmqEndpoint,
		c:                    c,
	}, nil
}

func (j *Junocashd) Terminate(ctx context.Context) error {
	if j == nil || j.c == nil {
		return nil
	}
	return j.c.Terminate(ctx)
}

func (st *IntegrationStack) Terminate(ctx context.Context) error {
	var firstErr error
	stop := func(c testcontainers.Container) {
		if c == nil {
			return
		}
		if err := c.Terminate(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	stop(st.junocashd)
	stop(st.kafka)
	stop(st.rabbitmq)
	stop(st.nats)
	stop(st.mysql)
	stop(st.postgres)
	return firstErr
}

func startJunocashd(ctx context.Context, rpcUser, rpcPass string) (testcontainers.Container, string, string, string, error) {
	version := defaultJunocashVersion

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    repoRoot(),
			Dockerfile: "docker/junocashd/Dockerfile",
			BuildArgs: map[string]*string{
				"JUNOCASH_VERSION": &version,
			},
		},
		ExposedPorts: []string{
			"8232/tcp",
			"28332/tcp",
		},
		Cmd: []string{
			"-regtest",
			"-server=1",
			"-txindex=1",
			"-daemon=0",
			"-listen=0",
			"-printtoconsole=1",
			"-datadir=/data",
			"-rpcbind=0.0.0.0",
			"-rpcallowip=0.0.0.0/0",
			"-rpcport=8232",
			"-rpcuser=" + rpcUser,
			"-rpcpassword=" + rpcPass,
			"-zmqpubhashblock=tcp://0.0.0.0:28332",
		},
		WaitingFor: wait.ForListeningPort(nat.Port("8232/tcp")).WithStartupTimeout(60 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", "", "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", "", "", err
	}

	rpcPort, err := c.MappedPort(ctx, nat.Port("8232/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", "", "", err
	}

	zmqPort, err := c.MappedPort(ctx, nat.Port("28332/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", "", "", err
	}

	id := c.GetContainerID()

	rpcURL := fmt.Sprintf("http://%s:%s", host, rpcPort.Port())
	zmqEndpoint := fmt.Sprintf("tcp://%s:%s", host, zmqPort.Port())

	return c, rpcURL, zmqEndpoint, id, nil
}

func startPostgres(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultPostgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     defaultPostgresUser,
			"POSTGRES_PASSWORD": defaultPostgresPassword,
			"POSTGRES_DB":       defaultPostgresDB,
		},
		WaitingFor: wait.ForListeningPort(nat.Port("5432/tcp")).WithStartupTimeout(60 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, nat.Port("5432/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", defaultPostgresUser, defaultPostgresPassword, host, port.Port(), defaultPostgresDB)
	return c, dsn, nil
}

func startMySQL(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultMySQLImage,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": defaultMySQLRootPass,
			"MYSQL_DATABASE":      defaultMySQLDB,
			"MYSQL_USER":          defaultMySQLUser,
			"MYSQL_PASSWORD":      defaultMySQLPassword,
		},
		WaitingFor: wait.ForListeningPort(nat.Port("3306/tcp")).WithStartupTimeout(90 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, nat.Port("3306/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}

	rootDSN := fmt.Sprintf("root:%s@tcp(%s:%s)/mysql", defaultMySQLRootPass, host, port.Port())
	return c, rootDSN, nil
}

func startNATS(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultNATSImage,
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("4222/tcp")).WithStartupTimeout(30 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, nat.Port("4222/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}

	return c, fmt.Sprintf("nats://%s:%s", host, port.Port()), nil
}

func startRabbitMQ(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        defaultRabbitMQImage,
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("5672/tcp")).WithStartupTimeout(60 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, nat.Port("5672/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}

	return c, fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port()), nil
}

func startKafka(ctx context.Context) (testcontainers.Container, string, error) {
	hostPort, err := freePort()
	if err != nil {
		return nil, "", err
	}

	req := testcontainers.ContainerRequest{
		Image: defaultKafkaImage,
		ExposedPorts: []string{
			strconv.Itoa(hostPort) + ":9092/tcp",
		},
		Env: map[string]string{
			"KAFKA_CFG_NODE_ID":                          "0",
			"KAFKA_CFG_PROCESS_ROLES":                    "controller,broker",
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS":         "0@localhost:9093",
			"KAFKA_CFG_LISTENERS":                        "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_CFG_ADVERTISED_LISTENERS":             fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", hostPort),
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP":   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CFG_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
			"KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE":        "true",
			"ALLOW_PLAINTEXT_LISTENER":                   "yes",
			"KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
		},
		WaitingFor: wait.ForListeningPort(nat.Port("9092/tcp")).WithStartupTimeout(120 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, "", err
	}

	return c, fmt.Sprintf("%s:%d", host, hostPort), nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
