// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:req REQ-WS-001
//fusa:req REQ-WS-002
//fusa:req REQ-WS-003
//fusa:req REQ-WS-004
//fusa:req REQ-WS-005
//fusa:req REQ-WS-006

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// wsGUID is the RFC 6455 magic value used to compute Sec-WebSocket-Accept.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WebSocket opcodes (RFC 6455 §5.2).
const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1
	wsOpBinary       = 0x2
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA
)

// DialWS connects to an MQTT broker over WebSocket (MQTT-over-WS, MQTT §6).
// rawURL is a ws:// or wss:// URL, e.g. "ws://broker:8080/mqtt". MQTT control
// packets are carried in WebSocket binary frames with the "mqtt" subprotocol.
//
// For wss:// the TLS config from WithTLS is used, or a default deriving
// ServerName from the URL host. The conventional MQTT-over-WS ports are 8080
// (ws) and 8081/443 (wss), but any port in the URL is honoured.
//
//fusa:req REQ-WS-001
//fusa:req REQ-WS-002
func DialWS(rawURL string, opts ...Option) (mqtt.Client, error) {
	o := newOptions(opts)

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("mqtt/v3: parse ws url %q: %w", rawURL, err)
	}
	secure := false
	switch u.Scheme {
	case "ws":
	case "wss":
		secure = true
	default:
		return nil, fmt.Errorf("mqtt/v3: unsupported ws scheme %q (want ws or wss)", u.Scheme)
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.dialTimeout)
	defer cancel()

	conn, err := dialWebSocket(ctx, u, secure, o)
	if err != nil {
		return nil, err
	}
	return newClient(conn, o)
}

// dialWebSocket dials TCP (optionally TLS), performs the RFC 6455 client
// handshake requesting the "mqtt" subprotocol, and returns a net.Conn that
// frames the MQTT byte stream as WebSocket binary frames.
//
//fusa:req REQ-WS-002
//fusa:req REQ-WS-003
func dialWebSocket(ctx context.Context, u *url.URL, secure bool, o *options) (net.Conn, error) {
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if secure {
			port = "443"
		} else {
			port = "80"
		}
	}
	addr := net.JoinHostPort(host, port)

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt/v3: dial %s: %w", addr, err)
	}
	if secure {
		cfg := o.tlsConfig
		if cfg == nil {
			cfg = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		}
		tlsConn := tls.Client(conn, cfg)
		if herr := tlsConn.HandshakeContext(ctx); herr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("mqtt/v3: TLS handshake with %s: %w", addr, herr)
		}
		conn = tlsConn
	}

	br, err := wsHandshake(conn, u, host)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &wsConn{Conn: conn, br: br}, nil
}

// wsHandshake performs the RFC 6455 opening handshake over conn and returns the
// buffered reader holding any bytes already read past the response headers.
//
//fusa:req REQ-WS-003
func wsHandshake(conn net.Conn, u *url.URL, host string) (*bufio.Reader, error) {
	keyRaw := make([]byte, 16)
	if _, err := rand.Read(keyRaw); err != nil {
		return nil, fmt.Errorf("mqtt/v3: ws key: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(keyRaw)

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		return nil, fmt.Errorf("mqtt/v3: ws request: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("mqtt/v3: ws response: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("mqtt/v3: ws handshake status %d (want 101)", resp.StatusCode)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		return nil, fmt.Errorf("mqtt/v3: ws handshake missing Upgrade: websocket")
	}
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wsAcceptKey(key) {
		return nil, fmt.Errorf("mqtt/v3: ws handshake bad Sec-WebSocket-Accept")
	}
	return br, nil
}

// wsAcceptKey computes the RFC 6455 Sec-WebSocket-Accept value for a client key.
//
//fusa:req REQ-WS-003
func wsAcceptKey(key string) string {
	h := sha1.New() //nolint:gosec // SHA-1 is mandated by RFC 6455 for this handshake
	_, _ = io.WriteString(h, key+wsGUID)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// wsConn adapts a WebSocket connection to net.Conn, framing the MQTT byte
// stream as binary frames. Client→server frames are masked per RFC 6455 §5.3.
//
//fusa:req REQ-WS-004
type wsConn struct {
	net.Conn
	br      *bufio.Reader
	readBuf []byte     // unconsumed payload from the current data frame
	writeMu sync.Mutex // serialises frame writes
}

// Read returns bytes from the MQTT stream, reading and unmasking WebSocket data
// frames as needed and transparently answering control frames.
//
//fusa:req REQ-WS-004
//fusa:req REQ-WS-005
func (w *wsConn) Read(p []byte) (int, error) {
	for len(w.readBuf) == 0 {
		opcode, payload, err := w.readFrame()
		if err != nil {
			return 0, err
		}
		switch opcode {
		case wsOpBinary, wsOpText, wsOpContinuation:
			w.readBuf = payload
		case wsOpPing:
			if err := w.writeFrame(wsOpPong, payload); err != nil {
				return 0, err
			}
		case wsOpPong:
			// ignore
		case wsOpClose:
			return 0, io.EOF
		default:
			return 0, fmt.Errorf("mqtt/v3: ws unknown opcode 0x%x", opcode)
		}
	}
	n := copy(p, w.readBuf)
	w.readBuf = w.readBuf[n:]
	return n, nil
}

// Write sends p as a single masked WebSocket binary frame.
//
//fusa:req REQ-WS-004
func (w *wsConn) Write(p []byte) (int, error) {
	if err := w.writeFrame(wsOpBinary, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close sends a best-effort WebSocket close frame and closes the connection.
//
//fusa:req REQ-WS-006
func (w *wsConn) Close() error {
	_ = w.writeFrame(wsOpClose, nil)
	return w.Conn.Close()
}

// readFrame reads one WebSocket frame from the buffered reader.
//
//fusa:req REQ-WS-005
func (w *wsConn) readFrame() (byte, []byte, error) {
	return readWSFrame(w.br)
}

// writeFrame writes a single masked WebSocket frame (client → server).
//
//fusa:req REQ-WS-004
func (w *wsConn) writeFrame(opcode byte, payload []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return writeWSFrame(w.Conn, opcode, payload, true)
}

// readWSFrame reads one WebSocket frame and returns its opcode and unmasked
// payload. Fragmented control frames are not expected from a conformant peer.
//
//fusa:req REQ-WS-005
func readWSFrame(r io.Reader) (byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	opcode := hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7F)

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return opcode, payload, nil
}

// writeWSFrame writes a single WebSocket frame. When mask is true the payload is
// masked with a fresh key (required for client → server frames per RFC 6455).
//
//fusa:req REQ-WS-004
func writeWSFrame(conn io.Writer, opcode byte, payload []byte, mask bool) error {
	hdr := []byte{0x80 | opcode} // FIN=1
	n := len(payload)
	maskBit := byte(0)
	if mask {
		maskBit = 0x80
	}
	switch {
	case n < 126:
		hdr = append(hdr, maskBit|byte(n))
	case n < 1<<16:
		hdr = append(hdr, maskBit|126)
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(n))
	default:
		hdr = append(hdr, maskBit|127)
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}

	out := payload
	if mask {
		var maskKey [4]byte
		if _, err := rand.Read(maskKey[:]); err != nil {
			return fmt.Errorf("mqtt/v3: ws mask: %w", err)
		}
		hdr = append(hdr, maskKey[:]...)
		out = make([]byte, n)
		for i := range payload {
			out[i] = payload[i] ^ maskKey[i%4]
		}
	}

	if _, err := conn.Write(append(hdr, out...)); err != nil {
		return err
	}
	return nil
}

// wsConn satisfies net.Conn: Read/Write/Close are overridden above and the
// remaining methods (deadlines, addresses) come from the embedded net.Conn.
var _ net.Conn = (*wsConn)(nil)
