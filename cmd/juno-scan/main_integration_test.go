//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abdullah1738/juno-scan/internal/config"
	"github.com/Abdullah1738/juno-scan/internal/testutil"
)

func TestRun_GracefulShutdownDuringScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	jd, err := testutil.StartJunocashd(ctx, testutil.JunocashdConfig{})
	if err != nil {
		if errors.Is(err, testutil.ErrJunocashdNotFound) || errors.Is(err, testutil.ErrJunocashCLIOnPath) || errors.Is(err, testutil.ErrListenNotAllowed) {
			t.Skip(err.Error())
		}
		t.Fatalf("StartJunocashd: %v", err)
	}
	defer func() { _ = jd.Stop(context.Background()) }()

	// The pinned linux/amd64 regtest daemon is emulated on arm64 developer
	// machines and can take longer than 30 seconds to generate this fixture.
	generateCtx, generateCancel := context.WithTimeout(ctx, 90*time.Second)
	defer generateCancel()
	if out, err := jd.CLICommand(generateCtx, "generate", "50").CombinedOutput(); err != nil {
		t.Fatalf("generate: %v\n%s", err, out)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listenAddr := ln.Addr().String()
	_ = ln.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(runCtx, config.Config{
			DBDriver:      "rocksdb",
			DBPath:        filepath.Join(t.TempDir(), "db"),
			RPCURL:        jd.RPCURL,
			RPCUser:       jd.RPCUser,
			RPCPassword:   jd.RPCPassword,
			ListenAddr:    listenAddr,
			UAHRP:         "jregtest",
			PollInterval:  5 * time.Millisecond,
			Confirmations: 2,
			BrokerDriver:  "none",
		})
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(20 * time.Second)
	sawProgress := false

	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listenAddr+"/v1/health", nil)
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var body struct {
			ScannedHeight *int64 `json:"scanned_height"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			_ = resp.Body.Close()
			t.Fatalf("decode health: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("health status=%d", resp.StatusCode)
		}
		if body.ScannedHeight != nil && *body.ScannedHeight > 0 {
			sawProgress = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !sawProgress {
		t.Fatalf("scanner never reported in-progress catch-up before timeout")
	}

	runCancel()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for graceful shutdown")
	}
}
