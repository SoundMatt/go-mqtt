// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v3

// In-process fake-broker tests for the MQTT v3.1.1 client connection lifecycle:
// CONNECT/CONNACK handshake, error paths, DISCONNECT on Close, keepalive PINGREQ,
// SUBSCRIBE emission, subscription teardown on close/TCP-loss, QoS 0 ordering,
// and tolerance of unknown packets. They exercise client.go without a broker.

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	mqtt "github.com/SoundMatt/go-mqtt"
)

// acceptWithCONNACK accepts one client, reads its CONNECT, and replies with a
// CONNACK carrying the given return code. It returns the live connection.
func acceptWithCONNACK(t *testing.T, fb *fakeBroker, returnCode byte) {
	t.Helper()
	conn, err := fb.ln.Accept()
	if err != nil {
		return
	}
	fb.conn = conn
	fb.readPacket(t) // CONNECT
	_, _ = conn.Write([]byte{pktCONNACK, 0x02, 0x00, returnCode})
}

// TestDialHandshake verifies Dial sends CONNECT and only returns once a success
// CONNACK is received.
//
//fusa:test REQ-CONN-001
//fusa:test REQ-CONN-002
func TestDialHandshake(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() {})

	c, err := Dial(fb.addr(), WithClientID("h"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = c.Close()
}

// TestDialConnectionRefused verifies Dial returns an error when no broker is
// listening at the address.
//
//fusa:test REQ-CONN-003
func TestDialConnectionRefused(t *testing.T) {
	// Reserve a port, then close it so the dial is refused.
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	if _, err := Dial(addr, WithClientID("r"), WithKeepalive(0), WithDialTimeout(time.Second)); err == nil {
		t.Fatal("Dial to a closed port succeeded, want error")
	}
}

// TestDialBadCONNACK verifies Dial returns an error on a non-zero CONNACK return
// code and closes the TCP connection after the failed handshake.
//
//fusa:test REQ-CONN-004
//fusa:test REQ-CONN-005
func TestDialBadCONNACK(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	go acceptWithCONNACK(t, fb, 0x05) // 0x05 = not authorized

	if _, err := Dial(fb.addr(), WithClientID("b"), WithKeepalive(0)); err == nil {
		t.Fatal("Dial succeeded on non-zero CONNACK return code, want error")
	}

	// REQ-CONN-004: the client must have closed its TCP connection, so the
	// broker side now reads EOF.
	deadline := time.Now().Add(time.Second)
	for fb.conn == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fb.conn != nil {
		_ = fb.conn.SetReadDeadline(time.Now().Add(time.Second))
		var buf [1]byte
		if _, err := fb.conn.Read(buf[:]); err == nil {
			t.Error("connection still open after failed handshake, want closed")
		}
	}
}

// TestCloseSendsDisconnect verifies Close sends a DISCONNECT and then closes the
// TCP connection.
//
//fusa:test REQ-CONN-006
//fusa:test REQ-CONN-007
func TestCloseSendsDisconnect(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	gotDisconnect := make(chan bool, 1)
	fb.serve(t, func() {
		hdr, _ := fb.readPacket(t)
		gotDisconnect <- hdr == pktDISCONNECT
	})

	c, err := Dial(fb.addr(), WithClientID("d"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case ok := <-gotDisconnect:
		if !ok {
			t.Error("first packet after CONNACK was not DISCONNECT")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not receive DISCONNECT")
	}
}

// TestCloseIdempotent verifies Close can be called more than once without error
// or panic.
//
//fusa:test REQ-CONN-008
func TestCloseIdempotent(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() {})

	c, err := Dial(fb.addr(), WithClientID("i"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	_ = c.Close() // must not panic
}

// TestKeepalivePing verifies the client emits PINGREQ at the keepalive interval
// and that the keepalive goroutine stops after Close.
//
//fusa:test REQ-CONN-009
//fusa:test REQ-CONN-010
func TestKeepalivePing(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	pings := make(chan struct{}, 8)
	fb.serve(t, func() {
		for {
			hdr, _ := fb.readPacket(t)
			if hdr == 0 {
				return // connection closed
			}
			if hdr == pktPINGREQ {
				select {
				case pings <- struct{}{}:
				default:
				}
			}
		}
	})

	c, err := Dial(fb.addr(), WithClientID("k"), WithKeepalive(30*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Expect at least one PINGREQ within a few intervals.
	select {
	case <-pings:
	case <-time.After(2 * time.Second):
		t.Fatal("no PINGREQ received within keepalive window")
	}

	_ = c.Close()
	// Drain any ping already in flight, then assert no new pings arrive.
	time.Sleep(50 * time.Millisecond)
	for len(pings) > 0 {
		<-pings
	}
	select {
	case <-pings:
		t.Error("PINGREQ received after Close — keepalive goroutine still running")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestSubscribeSendsPacket verifies Subscribe emits a SUBSCRIBE packet to the
// broker.
//
//fusa:test REQ-SUB-006
func TestSubscribeSendsPacket(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	gotSub := make(chan bool, 1)
	fb.serve(t, func() {
		hdr, body := fb.readPacket(t)
		gotSub <- hdr == pktSUBSCRIBE && containsBytes(body, []byte("sensors/#"))
	})

	c, err := Dial(fb.addr(), WithClientID("s"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Subscribe("sensors/#", mqtt.AtMostOnce); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case ok := <-gotSub:
		if !ok {
			t.Error("did not receive a SUBSCRIBE packet for sensors/#")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not receive SUBSCRIBE")
	}
}

// TestSubChannelsClosedOnClose verifies all active subscription channels are
// closed when the client is closed.
//
//fusa:test REQ-SAFETY-007
func TestSubChannelsClosedOnClose(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() { _, _ = fb.readPacket(t) }) // drain SUBSCRIBE

	c, err := Dial(fb.addr(), WithClientID("c"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = c.Close()

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Error("subscription channel delivered a message, want closed")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel not closed after Close")
	}
}

// TestSubChannelsClosedOnTCPLoss verifies subscription channels are closed when
// the underlying TCP connection is lost.
//
//fusa:test REQ-SAFETY-006
//fusa:test REQ-FAULT-003
func TestSubChannelsClosedOnTCPLoss(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() { _, _ = fb.readPacket(t) }) // drain SUBSCRIBE

	c, err := Dial(fb.addr(), WithClientID("l"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Drop the connection from the broker side.
	if fb.conn != nil {
		_ = fb.conn.Close()
	}

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Error("subscription channel delivered a message, want closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription channel not closed after TCP loss")
	}
}

// TestUnknownPacketIgnored verifies an unexpected/unknown packet type from the
// broker is ignored rather than crashing the client.
//
//fusa:test REQ-FAULT-002
func TestUnknownPacketIgnored(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() {
		// Send a reserved/unknown packet type (0xF0) with an empty body.
		_, _ = fb.conn.Write([]byte{0xF0, 0x00})
		// Follow with a valid PUBLISH to prove the client is still processing.
		_, _ = fb.conn.Write(buildPUBLISH("a/b", []byte("ok"), 0, false, 0))
	})

	c, err := Dial(fb.addr(), WithClientID("u"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	sub, err := c.Subscribe("a/#", mqtt.AtMostOnce)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case m := <-sub.C():
		if string(m.Payload) != "ok" {
			t.Errorf("payload = %q, want ok", m.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not recover after an unknown packet")
	}
}

// TestPublishOrderQoS0 verifies QoS 0 PUBLISH packets sent from one goroutine are
// written to the connection in publication order.
//
//fusa:test REQ-ORDER-002
func TestPublishOrderQoS0(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	const n = 20
	order := make(chan byte, n)
	fb.serve(t, func() {
		for range n {
			hdr, body := fb.readPacket(t)
			if hdr&0xF0 != pktPUBLISH&0xF0 {
				continue
			}
			// body: [topicLen(2)][topic][payload]; topic "t" → payload at [3:].
			if len(body) >= 4 {
				order <- body[len(body)-1]
			}
		}
	})

	c, err := Dial(fb.addr(), WithClientID("o"), WithKeepalive(0))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	for i := range n {
		if err := c.Publish(context.Background(), "t", mqtt.AtMostOnce, []byte{byte(i)}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	for i := range n {
		select {
		case got := <-order:
			if got != byte(i) {
				t.Fatalf("message %d out of order: got %d", i, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

// TestGoroutinesExitAfterClose verifies the read and keepalive goroutines started
// by Dial exit after Close (no goroutine leak across connect/disconnect cycles).
//
//fusa:test REQ-LEAK-001
func TestGoroutinesExitAfterClose(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()
	fb.serve(t, func() {
		for {
			if hdr, _ := fb.readPacket(t); hdr == 0 {
				return
			}
		}
	})

	before := runtime.NumGoroutine()
	c, err := Dial(fb.addr(), WithClientID("g"), WithKeepalive(20*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = c.Close()

	// Allow the read and ping goroutines to observe the closed done channel.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return // goroutines reclaimed
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutines did not return after Close: before=%d now=%d", before, runtime.NumGoroutine())
}

// containsBytes reports whether sub appears within b.
func containsBytes(b, sub []byte) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(b); i++ {
		match := true
		for j := range sub {
			if b[i+j] != sub[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
