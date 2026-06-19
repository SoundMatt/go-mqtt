// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

//fusa:test REQ-WS-001
//fusa:test REQ-WS-002
//fusa:test REQ-WS-003
//fusa:test REQ-WS-004
//fusa:test REQ-WS-005
//fusa:test REQ-WS-006

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// serverWS adapts a hijacked connection to the server side of a WebSocket: it
// reads masked client frames and writes unmasked server frames, presenting the
// MQTT byte stream as a net.Conn for the fakeBroker helpers.
type serverWS struct {
	net.Conn
	br      *bufio.Reader
	readBuf []byte
}

func (s *serverWS) Read(p []byte) (int, error) {
	for len(s.readBuf) == 0 {
		op, payload, err := readWSFrame(s.br)
		if err != nil {
			return 0, err
		}
		switch op {
		case wsOpBinary, wsOpText, wsOpContinuation:
			s.readBuf = payload
		case wsOpPing:
			_ = writeWSFrame(s.Conn, wsOpPong, payload, false)
		case wsOpClose:
			return 0, io.EOF
		}
	}
	n := copy(p, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

func (s *serverWS) Write(p []byte) (int, error) {
	return len(p), writeWSFrame(s.Conn, wsOpBinary, p, false)
}

// newWSBroker starts an httptest server that upgrades to WebSocket and then runs
// handler with a fakeBroker bound to the framed connection. handler runs after
// the WS handshake but before CONNECT, mirroring fakeBroker.serve.
func newWSBroker(t *testing.T, handler func(fb *fakeBroker)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
			return
		}
		if proto := r.Header.Get("Sec-WebSocket-Protocol"); proto != "mqtt" {
			t.Errorf("Sec-WebSocket-Protocol = %q, want mqtt", proto)
		}
		key := r.Header.Get("Sec-WebSocket-Key")

		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n" +
			"Sec-WebSocket-Protocol: mqtt\r\n\r\n"
		if _, err := io.WriteString(conn, resp); err != nil {
			_ = conn.Close()
			return
		}

		sc := &serverWS{Conn: conn, br: bufio.NewReader(conn)}
		fb := &fakeBroker{conn: sc}
		// Read CONNECT, send CONNACK, then run the per-test handler.
		fb.readPacket(t)
		if _, err := sc.Write([]byte{pktCONNACK, 0x02, 0x00, 0x00}); err != nil {
			return
		}
		if handler != nil {
			handler(fb)
		}
	}))
}

// wsURL converts an httptest http:// URL to a ws:// URL.
func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// TestDialWSConnect verifies the WebSocket handshake and MQTT CONNECT over WS.
//
//fusa:test REQ-WS-001
//fusa:test REQ-WS-003
func TestDialWSConnect(t *testing.T) {
	srv := newWSBroker(t, nil)
	defer srv.Close()

	c, err := DialWS(wsURL(srv.URL), WithClientID("ws-client"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()
}

// TestDialWSBadScheme verifies non-ws schemes are rejected.
//
//fusa:test REQ-WS-001
func TestDialWSBadScheme(t *testing.T) {
	if _, err := DialWS("http://example/mqtt"); err == nil {
		t.Error("DialWS with http scheme: expected error, got nil")
	}
}

// TestDialWSPublish verifies an MQTT PUBLISH is carried in a WS binary frame and
// reaches the broker intact.
//
//fusa:test REQ-WS-004
func TestDialWSPublish(t *testing.T) {
	got := make(chan mqtt.Message, 1)
	srv := newWSBroker(t, func(fb *fakeBroker) {
		hdr, body := fb.readPacket(t)
		if hdr&0xF0 != pktPUBLISH&0xF0 {
			t.Errorf("expected PUBLISH, got 0x%02x", hdr)
			return
		}
		topicLen := int(body[0])<<8 | int(body[1])
		topic := string(body[2 : 2+topicLen])
		got <- mqtt.Message{Topic: topic, Payload: body[2+topicLen:]}
	})
	defer srv.Close()

	c, err := DialWS(wsURL(srv.URL), WithClientID("ws-pub"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Publish(t.Context(), "a/b", mqtt.AtMostOnce, []byte("ws-hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-got:
		if msg.Topic != "a/b" {
			t.Errorf("topic = %q, want a/b", msg.Topic)
		}
		if string(msg.Payload) != "ws-hello" {
			t.Errorf("payload = %q, want ws-hello", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for PUBLISH over WS")
	}
}

// TestDialWSSubscribeDeliver verifies a broker PUBLISH (server → client WS
// frame) is delivered to a subscription.
//
//fusa:test REQ-WS-005
func TestDialWSSubscribeDeliver(t *testing.T) {
	srv := newWSBroker(t, func(fb *fakeBroker) {
		fb.readPacket(t) // SUBSCRIBE
		// Server sends a QoS 0 PUBLISH as an unmasked WS binary frame.
		if _, err := fb.conn.Write(buildPUBLISH("a/b", []byte("from-broker"), 0, false, 0)); err != nil {
			t.Errorf("server write: %v", err)
		}
	})
	defer srv.Close()

	c, err := DialWS(wsURL(srv.URL), WithClientID("ws-sub"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	select {
	case msg := <-sub.C():
		if string(msg.Payload) != "from-broker" {
			t.Errorf("payload = %q, want from-broker", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery over WS")
	}
}

// TestDialWSLargeFrame exercises the 16-bit extended length path with a payload
// larger than 125 bytes, split across the WS framing.
//
//fusa:test REQ-WS-004
//fusa:test REQ-WS-005
func TestDialWSLargeFrame(t *testing.T) {
	big := strings.Repeat("x", 1000)
	got := make(chan string, 1)
	srv := newWSBroker(t, func(fb *fakeBroker) {
		hdr, body := fb.readPacket(t)
		if hdr&0xF0 != pktPUBLISH&0xF0 {
			t.Errorf("expected PUBLISH, got 0x%02x", hdr)
			return
		}
		topicLen := int(body[0])<<8 | int(body[1])
		got <- string(body[2+topicLen:])
	})
	defer srv.Close()

	c, err := DialWS(wsURL(srv.URL), WithClientID("ws-big"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Publish(t.Context(), "big/topic", mqtt.AtMostOnce, []byte(big)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case payload := <-got:
		if payload != big {
			t.Errorf("payload len = %d, want %d", len(payload), len(big))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for large frame")
	}
}

// TestWSAcceptKey verifies the RFC 6455 example accept-key computation.
//
//fusa:test REQ-WS-003
func TestWSAcceptKey(t *testing.T) {
	// From RFC 6455 §1.3.
	if got := wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Errorf("wsAcceptKey = %q, want s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", got)
	}
}
