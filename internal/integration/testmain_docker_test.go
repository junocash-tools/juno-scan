//go:build integration && docker

package integration_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/testutil/containers"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	st, err := containers.StartIntegrationStack(ctx)
	if err != nil {
		log.Printf("start containers: %v", err)
		os.Exit(1)
	}

	os.Setenv("JUNO_TEST_DOCKER", "1")
	os.Setenv("JUNO_TEST_RPC_URL", st.RPCURL)
	os.Setenv("JUNO_TEST_RPC_USER", st.RPCUser)
	os.Setenv("JUNO_TEST_RPC_PASS", st.RPCPassword)
	os.Setenv("JUNO_TEST_JUNOCASHD_CONTAINER", st.JunocashdContainerID)
	os.Setenv("JUNO_TEST_JUNOCASHD_DATADIR", "/data")
	os.Setenv("JUNO_TEST_JUNOCASHD_RPC_PORT", "8232")
	os.Setenv("JUNO_TEST_ZMQ_HASHBLOCK", st.ZMQHashBlockEndpoint)
	os.Setenv("JUNO_TEST_POSTGRES_DSN", st.PostgresDSN)
	os.Setenv("JUNO_TEST_MYSQL_ROOT_DSN", st.MySQLRootDSN)
	os.Setenv("JUNO_TEST_NATS_URL", st.NATSURL)
	os.Setenv("JUNO_TEST_RABBITMQ_URL", st.RabbitMQURL)
	os.Setenv("JUNO_TEST_KAFKA_BROKERS", st.KafkaBrokers)

	code := m.Run()

	termCtx, termCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer termCancel()
	if err := st.Terminate(termCtx); err != nil {
		log.Printf("terminate containers: %v", err)
	}

	os.Exit(code)
}
