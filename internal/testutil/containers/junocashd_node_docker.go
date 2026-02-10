//go:build docker

package containers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	build "github.com/docker/docker/api/types/build"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type JunocashdNodeConfig struct {
	Network string
	Alias   string

	RPCUser     string
	RPCPassword string

	P2PPort int

	ExtraArgs []string
}

type JunocashdNode struct {
	ContainerID string

	RPCURL      string
	RPCUser     string
	RPCPassword string

	Network string
	Alias   string
	P2PPort int

	c testcontainers.Container
}

func StartJunocashdNode(ctx context.Context, cfg JunocashdNodeConfig) (*JunocashdNode, error) {
	rpcUser := strings.TrimSpace(cfg.RPCUser)
	if rpcUser == "" {
		rpcUser = defaultRPCUser
	}
	rpcPass := strings.TrimSpace(cfg.RPCPassword)
	if rpcPass == "" {
		rpcPass = defaultRPCPassword
	}

	p2pPort := cfg.P2PPort
	if p2pPort <= 0 {
		p2pPort = 8233
	}

	args := []string{
		"-regtest",
		"-server=1",
		"-txindex=1",
		"-daemon=0",
		"-listen=1",
		"-bind=0.0.0.0",
		"-port=" + strconv.Itoa(p2pPort),
		"-printtoconsole=1",
		"-txunpaidactionlimit=10000",
		"-blockunpaidactionlimit=0",
		"-txexpirydelta=4",
		"-datadir=/data",
		"-rpcbind=0.0.0.0",
		"-rpcallowip=0.0.0.0/0",
		"-rpcport=8232",
		"-rpcuser=" + rpcUser,
		"-rpcpassword=" + rpcPass,
	}
	args = append(args, cfg.ExtraArgs...)

	version := defaultJunocashVersion
	req := testcontainers.ContainerRequest{
		ImagePlatform: "linux/amd64",
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    repoRoot(),
			Dockerfile: "docker/junocashd/Dockerfile",
			BuildArgs: map[string]*string{
				"JUNOCASH_VERSION": &version,
			},
			BuildOptionsModifier: func(opts *build.ImageBuildOptions) {
				opts.Platform = "linux/amd64"
				opts.Version = build.BuilderBuildKit
			},
		},
		ExposedPorts: []string{
			"8232/tcp",
			fmt.Sprintf("%d/tcp", p2pPort),
		},
		Cmd:        args,
		WaitingFor: wait.ForListeningPort(nat.Port("8232/tcp")).WithStartupTimeout(60 * time.Second),
	}

	if strings.TrimSpace(cfg.Network) != "" {
		req.Networks = []string{strings.TrimSpace(cfg.Network)}
	}
	if strings.TrimSpace(cfg.Network) != "" && strings.TrimSpace(cfg.Alias) != "" {
		req.NetworkAliases = map[string][]string{
			strings.TrimSpace(cfg.Network): {strings.TrimSpace(cfg.Alias)},
		}
	}

	if os.Getenv("JUNO_TEST_LOG") != "" {
		req.FromDockerfile.BuildLogWriter = os.Stdout
		req.LogConsumerCfg = &testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&testcontainers.StdoutLogConsumer{}},
		}
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}

	rpcPort, err := c.MappedPort(ctx, nat.Port("8232/tcp"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}

	n := &JunocashdNode{
		ContainerID: c.GetContainerID(),
		RPCURL:      fmt.Sprintf("http://%s:%s", host, rpcPort.Port()),
		RPCUser:     rpcUser,
		RPCPassword: rpcPass,
		Network:     strings.TrimSpace(cfg.Network),
		Alias:       strings.TrimSpace(cfg.Alias),
		P2PPort:     p2pPort,
		c:           c,
	}

	return n, nil
}

func (n *JunocashdNode) Terminate(ctx context.Context) error {
	if n == nil || n.c == nil {
		return nil
	}
	return n.c.Terminate(ctx)
}

func (n *JunocashdNode) P2PAddress(ctx context.Context) (string, error) {
	if strings.TrimSpace(n.Alias) != "" {
		return fmt.Sprintf("%s:%d", strings.TrimSpace(n.Alias), n.P2PPort), nil
	}
	ip, err := n.c.ContainerIP(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", ip, n.P2PPort), nil
}

func (n *JunocashdNode) ExecCLI(ctx context.Context, args ...string) ([]byte, error) {
	if n == nil || n.c == nil {
		return nil, fmt.Errorf("junocashd: container is nil")
	}

	cmd := append([]string{
		"junocash-cli",
		"-regtest",
		"-datadir=/data",
		"-rpcuser=" + n.RPCUser,
		"-rpcpassword=" + n.RPCPassword,
		"-rpcport=8232",
	}, args...)

	exitCode, reader, err := n.c.Exec(ctx, cmd)
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, bytes.NewReader(raw)); err != nil {
		stdout.Write(raw)
	}
	if exitCode != 0 {
		msg := bytes.TrimSpace(stderr.Bytes())
		if len(msg) == 0 {
			msg = bytes.TrimSpace(stdout.Bytes())
		}
		return nil, fmt.Errorf("junocash-cli exit %d: %s", exitCode, msg)
	}
	return stdout.Bytes(), nil
}
