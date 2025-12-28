package zmq

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

type NotifyConfig struct {
	Endpoint       string
	Topic          string
	ReconnectDelay time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func Notify(ctx context.Context, cfg NotifyConfig, out chan<- struct{}, logf func(string, ...any)) error {
	if out == nil {
		return errors.New("zmq: out channel is nil")
	}
	addr, err := ParseEndpoint(cfg.Endpoint)
	if err != nil {
		return err
	}
	if cfg.Topic == "" {
		return errors.New("zmq: topic is required")
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = 2 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}

	for ctx.Err() == nil {
		if err := notifyOnce(ctx, addr, cfg.Topic, cfg.ReadTimeout, cfg.WriteTimeout, out); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if logf != nil {
				logf("zmq notify error: %v", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(cfg.ReconnectDelay):
			}
		}
	}

	return nil
}

func notifyOnce(ctx context.Context, addr string, topic string, readTimeout, writeTimeout time.Duration, out chan<- struct{}) error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := handshakeNullV3(conn, writeTimeout, readTimeout); err != nil {
		return err
	}

	if err := subscribe(conn, topic, writeTimeout); err != nil {
		return err
	}

	r := bufio.NewReader(conn)
	for ctx.Err() == nil {
		readDeadline(conn, readTimeout)
		frames, err := readMessage(r)
		if err != nil {
			return err
		}
		if len(frames) == 0 {
			continue
		}
		if bytes.Equal(frames[0], []byte(topic)) {
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func handshakeNullV3(conn net.Conn, writeTimeout, readTimeout time.Duration) error {
	g := greetingV3Null()

	// Signature + major version.
	writeDeadline(conn, writeTimeout)
	if _, err := conn.Write(g[:11]); err != nil {
		return fmt.Errorf("handshake: write greeting: %w", err)
	}

	var peer [64]byte
	readDeadline(conn, readTimeout)
	if _, err := ioReadFull(conn, peer[:11]); err != nil {
		return fmt.Errorf("handshake: read greeting: %w", err)
	}

	// Remaining greeting fields.
	writeDeadline(conn, writeTimeout)
	if _, err := conn.Write(g[11:]); err != nil {
		return fmt.Errorf("handshake: write greeting rest: %w", err)
	}
	readDeadline(conn, readTimeout)
	if _, err := ioReadFull(conn, peer[11:]); err != nil {
		return fmt.Errorf("handshake: read greeting rest: %w", err)
	}

	// Send READY.
	meta, err := encodeREADYMetadata("SUB")
	if err != nil {
		return err
	}

	var ready bytes.Buffer
	ready.WriteByte(byte(len("READY")))
	ready.WriteString("READY")
	ready.Write(meta)

	writeDeadline(conn, writeTimeout)
	if err := writeFrame(conn, true, false, ready.Bytes()); err != nil {
		return fmt.Errorf("handshake: send READY: %w", err)
	}

	// Receive peer READY (ignore contents).
	readDeadline(conn, readTimeout)
	_, _, _, err = readFrame(conn)
	if err != nil {
		return fmt.Errorf("handshake: read READY: %w", err)
	}

	return nil
}

func subscribe(conn net.Conn, topic string, writeTimeout time.Duration) error {
	body := append([]byte{0x01}, []byte(topic)...)
	writeDeadline(conn, writeTimeout)
	if err := writeFrame(conn, false, false, body); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	return nil
}

func readMessage(r *bufio.Reader) ([][]byte, error) {
	var frames [][]byte
	for {
		cmd, more, body, err := readFrame(r)
		if err != nil {
			return nil, err
		}
		if cmd {
			// Ignore commands in data stream.
			continue
		}
		frames = append(frames, body)
		if !more {
			return frames, nil
		}
	}
}

func ioReadFull(conn net.Conn, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := conn.Read(b[n:])
		if m > 0 {
			n += m
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
