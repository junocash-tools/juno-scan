package zmq

import (
	"bytes"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	if got, err := ParseEndpoint("tcp://127.0.0.1:28332"); err != nil || got != "127.0.0.1:28332" {
		t.Fatalf("ParseEndpoint tcp://: got=%q err=%v", got, err)
	}
	if got, err := ParseEndpoint("127.0.0.1:28332"); err != nil || got != "127.0.0.1:28332" {
		t.Fatalf("ParseEndpoint bare: got=%q err=%v", got, err)
	}
	if _, err := ParseEndpoint("ipc:///tmp/zmq.sock"); err == nil {
		t.Fatalf("expected error for ipc endpoint")
	}
	if _, err := ParseEndpoint("not-a-host"); err == nil {
		t.Fatalf("expected error for invalid endpoint")
	}
}

func TestGreetingV3Null(t *testing.T) {
	g := greetingV3Null()
	if len(g) != 64 {
		t.Fatalf("greeting length=%d want 64", len(g))
	}
	if g[0] != 0xFF || g[9] != 0x7F {
		t.Fatalf("unexpected signature bytes: %x ... %x", g[0], g[9])
	}
	if g[10] != 3 || g[11] != 0 {
		t.Fatalf("unexpected version: %d.%d", g[10], g[11])
	}
	if string(bytes.TrimRight(g[12:32], "\x00")) != "NULL" {
		t.Fatalf("unexpected mechanism: %q", g[12:32])
	}
}

func TestFrameRoundTrip_Short(t *testing.T) {
	var buf bytes.Buffer
	body := []byte("hello")
	if err := writeFrame(&buf, false, true, body); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	cmd, more, got, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if cmd {
		t.Fatalf("cmd=true want false")
	}
	if !more {
		t.Fatalf("more=false want true")
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body=%q want %q", string(got), string(body))
	}
}

func TestFrameRoundTrip_Long(t *testing.T) {
	var buf bytes.Buffer
	body := bytes.Repeat([]byte{0xAB}, 300)
	if err := writeFrame(&buf, true, false, body); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	cmd, more, got, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !cmd {
		t.Fatalf("cmd=false want true")
	}
	if more {
		t.Fatalf("more=true want false")
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch")
	}
}
