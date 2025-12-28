package zmq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

var (
	ErrEndpointInvalid = errors.New("zmq: invalid endpoint")
)

func ParseEndpoint(endpoint string) (string, error) {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		return "", ErrEndpointInvalid
	}
	if strings.HasPrefix(e, "tcp://") {
		e = strings.TrimPrefix(e, "tcp://")
	} else if strings.Contains(e, "://") {
		return "", ErrEndpointInvalid
	}

	host, port, err := net.SplitHostPort(e)
	if err != nil || host == "" || port == "" {
		return "", ErrEndpointInvalid
	}
	return e, nil
}

func greetingV3Null() [64]byte {
	var g [64]byte
	g[0] = 0xFF
	g[8] = 0x01
	g[9] = 0x7F
	g[10] = 3 // major
	g[11] = 0 // minor
	copy(g[12:32], []byte("NULL"))
	return g
}

func encodeREADYMetadata(socketType string) ([]byte, error) {
	if socketType == "" || len(socketType) > 255 {
		return nil, errors.New("zmq: invalid socket type")
	}

	var buf bytes.Buffer
	if err := writeProp(&buf, "Socket-Type", []byte(socketType)); err != nil {
		return nil, err
	}
	if err := writeProp(&buf, "Identity", nil); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeProp(w io.Writer, name string, value []byte) error {
	if name == "" || len(name) > 255 {
		return errors.New("zmq: invalid property name")
	}

	if err := writeByte(w, byte(len(name))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, name); err != nil {
		return err
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}
	_, err := w.Write(value)
	return err
}

func writeByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}

func writeFrame(w io.Writer, command bool, more bool, body []byte) error {
	if command && more {
		return errors.New("zmq: command frame cannot have more set")
	}

	bodyLen := uint64(len(body))
	long := bodyLen > 255

	flags := byte(0)
	if more {
		flags |= 0x01
	}
	if long {
		flags |= 0x02
	}
	if command {
		flags |= 0x04
	}

	if err := writeByte(w, flags); err != nil {
		return err
	}

	if !long {
		if err := writeByte(w, byte(bodyLen)); err != nil {
			return err
		}
	} else {
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], bodyLen)
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
	}

	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

func readFrame(r io.Reader) (command bool, more bool, body []byte, err error) {
	var flags [1]byte
	if _, err := io.ReadFull(r, flags[:]); err != nil {
		return false, false, nil, err
	}
	command = (flags[0] & 0x04) != 0
	more = (flags[0] & 0x01) != 0
	long := (flags[0] & 0x02) != 0

	var size uint64
	if !long {
		var n [1]byte
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return false, false, nil, err
		}
		size = uint64(n[0])
	} else {
		var n [8]byte
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return false, false, nil, err
		}
		size = binary.BigEndian.Uint64(n[:])
	}

	if size > 128*1024*1024 {
		return false, false, nil, fmt.Errorf("zmq: frame too large: %d", size)
	}

	if size == 0 {
		return command, more, nil, nil
	}

	body = make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return false, false, nil, err
	}
	return command, more, body, nil
}

func writeDeadline(c net.Conn, d time.Duration) {
	if d <= 0 {
		return
	}
	_ = c.SetWriteDeadline(time.Now().Add(d))
}

func readDeadline(c net.Conn, d time.Duration) {
	if d <= 0 {
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(d))
}
